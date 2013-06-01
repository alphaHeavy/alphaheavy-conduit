{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# OPTIONS_GHC -fno-warn-name-shadowing #-}
{-# OPTIONS_GHC -Wwarn #-}

module AlphaHeavy.Conduit
  ( chunk
  , flatten
  , groupBy
  , mapAccum
  , mergeSinks
  , mergeSinksBy
  , routeToSinksBy
  , routeToSinksBy_
  , mergeSources
  , intersectSourcesBy
  , mergeSourcesBy
  , mergeSourcesAsync
  , pipePush
  , pipePushM
  , sourceFromTMVar
  , sourceFromTBQueue
  , sourceFromTChan
  -- , sourceFromTQueue
  , unique
  , uniqueBy
  , passthroughSink
  , unlines
  ) where

import Prelude hiding (catch, mapM, unlines)
import Control.Applicative
import Control.Concurrent (ThreadId, forkIO, killThread)
import Control.Concurrent.STM
import Control.Exception
import Control.Monad (forM_, foldM, liftM, when)
import Control.Monad.Trans (lift, liftIO, MonadIO)
import Control.Monad.Trans.Resource (allocate, release)
import qualified Data.ByteString as B
import Data.Conduit as C
import Data.Conduit.Internal
import qualified Data.Conduit.List as Cl
import qualified Data.Foldable as Fold
import Data.Function (on)
import Data.List (insertBy, sortBy)
import qualified Data.Map as Map
import Data.Map (Map)
import Data.Monoid
import Data.String (IsString)
import Data.Traversable
import Data.Void (Void)

descending :: (a -> a -> Ordering) -> a -> a -> Ordering
descending cmp v1 v2 = v2 `cmp` v1 -- Reversing the arguments to compare sorts in the reverse order

mapAccum :: Monad m => (a -> b -> (a, c)) -> a -> GInfConduit b m c
mapAccum f inita = go inita where
  go acc = awaitE >>= either return (\input -> let (acc', val) = f acc input in yield val >> go acc')

-- | Unique a sorted 'Source'
unique
  :: (Eq a, Monad m)
  => GInfConduit a m a
unique = uniqueBy (==)

-- | Unique a sorted 'Source'
uniqueBy
  :: Monad m
  => (a -> a -> Bool)
  -> GInfConduit a m a
uniqueBy comp = init where
  init = do
    mval <- awaitE
    case mval of
      Left val  -> return val
      Right val -> step val

  step last = do
    mval <- awaitE
    case mval of
      Left val -> yield last >> return val
      Right val
        | comp val last -> step val
        | otherwise     -> yield last >> step val

runForward :: Monad m => Pipe l i o u m r -> m (Pipe l i o u m r)
runForward (PipeM mp) = runForward =<< mp
runForward x = return x

passthroughSink
  :: Monad m
  => Sink a m ()
  -> GConduit a m a
passthroughSink (NeedInput i c) = do
  mx <- await
  case mx of
    Just x ->
      -- check to see if we have any monadic actions to
      -- evaluate... to preserve ordering between the inner
      -- sink and the final sink in this conduit chain
      case i x of
        p@PipeM{} -> do
          -- run the inner sink before yielding
          np <- lift $ runForward p
          yield x
          passthroughSink np

        np -> do
          -- otherwise it's a pure action, order doesn't matter
          yield x
          passthroughSink np

    -- if we're done make sure to call the early termination
    -- pipe from the inner sink
    Nothing -> passthroughSink (c ())

passthroughSink (PipeM mp) =
  passthroughSink =<< lift mp

passthroughSink (Done r) =
  return r

passthroughSink (HaveOutput _ _ _) =
  fail "passthroughSink should not see a HaveOutput"

-- | Perform a merge-sort on a list of 'Source's
mergeSources
  :: (Ord o, Monoid u, Monad m)
  => [Pipe l i o u m ()]
  -> Pipe l i o u m ()
mergeSources = mergeSourcesBy compare

-- | Perform a merge-sort on a list of 'Source's
mergeSourcesBy
  :: forall l i o u m . (Monoid u, Monad m)
  => (o -> o -> Ordering)
  -> [Pipe l i o u m ()]
  -> Pipe l i o u m ()
mergeSourcesBy comp sources = pull $ sortBy compResults sources where
  compResults = compareSourceResults comp

  pull :: [Pipe l i o u m ()] -> Pipe l i o u m ()
  pull [next] =
    -- single source, no more merge sorts required
    next

  pull ((HaveOutput src _ val):xs) = do
    -- standard case, keep merging
    yield val
    pull (insertBy compResults src xs)

  pull ((PipeM mp):xs) = do
    src <- lift mp
    pull (insertBy compResults src xs)

  pull ((NeedInput _ c):xs) =
    fail "mergeSourcesBy found Source looking for input"
    -- pull (insertBy compResults (c ()) xs)

  pull ((Done _):xs) =
    pull xs

  pull [] =
    return mempty

compareSourceResults
  :: (o -> o -> Ordering)
  -> Pipe l i o u m r
  -> Pipe l i o u m r
  -> Ordering
compareSourceResults comp x y = case (x, y) of
  (HaveOutput _ _ lhs, HaveOutput _ _ rhs) -> comp lhs rhs
  (PipeM _,            _)                  -> LT
  (_,                  PipeM _)            -> GT
  (HaveOutput _ _ _,   _)                  -> LT
  (_,                  HaveOutput _ _ _)   -> GT
  (_,                  _)                  -> EQ

intersectSourcesBy :: forall a m . Monad m => (a -> a -> Ordering) -> [Source m a] -> Source m [a]
intersectSourcesBy comp sources = mergeSourcesBy comp' sources' $= group []
  where sourceCount = length sources
        sources' :: [Source m (a, Int)]
        sources' = map (\(src, idx) -> src $= Cl.map (,idx)) $ zip sources [0..]

        comp' :: (a, Int) -> (a, Int) -> Ordering
        comp' = (comp `on` fst) <> descending (compare `on` snd)

        group :: [a] -> Conduit (a, Int) m [a]
        group [] = await >>= maybe (return ()) (group . (:[]) . fst)
        group xs@(x:_) = do
          mnext <- await
          case mnext of
            Nothing -> when (length xs == sourceCount) (yield xs)
            Just (next, _) | EQ == comp next x -> group (next:xs)
                           | otherwise -> when (length xs == sourceCount) (yield xs) >> group [next]

pipePush
  :: Monad m
  => i
  -> Pipe i i o u m r
  -> Pipe i i o u m r
pipePush i (HaveOutput p c o) = HaveOutput (pipePush i p) c o
pipePush i (NeedInput p _)    = p i
pipePush i r@(Done _)         = Leftover r i
pipePush i (PipeM mp)         = PipeM (pipePush i `liftM` mp)

pipePushM
  :: Monad m
  => i
  -> Pipe i i o u m r
  -> m (Pipe i i o u m r)
pipePushM i (HaveOutput p c o) = pipePushM i p >>= \p' -> return $! HaveOutput p' c o
pipePushM i (NeedInput p _)    = return $! p i
pipePushM i r@(Done _)         = return $! Leftover r i
pipePushM i (PipeM mp)         = mp >>= \p -> pipePushM i p

mergeSinks
  :: (Monad m, Monoid r)
  => [Pipe i i Void () m r]
  -> Pipe i i Void () m r
mergeSinks =
  mergeSinksBy mappend mempty

-- |
-- Merge sinks in the order they complete
mergeSinksBy
  :: forall i m r r' . Monad m
  => (r' -> r -> r')
  -> r'
  -> [Pipe i i Void () m r]
  -> Pipe i i Void () m r'
mergeSinksBy combine = go where
  go :: r' -> [Pipe i i Void () m r ] -> Pipe i i Void () m r'
  go acc xs =
    NeedInput (feed acc xs) (const (fini acc xs))

  fini :: r' -> [Pipe i i Void () m r] -> Pipe i i Void () m r'
  fini =
    -- terminating early. run all the finalizers and combine the results
    foldM $ \ acc sink -> combine acc <$> lift (return () $$ sink)

  feed :: r' -> [Pipe i i Void () m r] -> i -> Pipe i i Void () m r'
  feed acc xs i =
    -- feed the input into each pipe
    let xs' = fmap (pipePush i) xs
    -- then run them forward to the next input state
    in step acc xs' []

  -- the actual merging step. this is done tail recursively rather than
  -- lazily to naturally evaluate the pipe thunks. consing each pipe into
  -- the pipe accumulator 'ys' forces the pipes to be evaluated in a
  -- zig-zag order as an input value is fed to the child sinks.
  step :: r' -> [Pipe i i Void () m r] -> [Pipe i i Void () m r] -> Pipe i i Void () m r'
  step acc [] ys =
    -- once the input has been fed into all sinks
    -- we need to get another input value to continue
    NeedInput (feed acc ys) (const (fini acc ys))

  step acc [x] [] =
    -- a single sink is left, terminate early and combine the result
    fmap (combine acc) x

  step acc (x:xs) ys =
    case x of
      NeedInput _ _ ->
        -- the sink is blocked on input, add it to the pipe accumulator
        step acc xs (x:ys)

      Done el ->
        -- the sink is done, combine the result and discard the pipe
        step (acc `combine` el) xs ys

      PipeM mp -> do
        -- run the action and recurse
        x' <- lift mp
        step acc (x':xs) ys

      HaveOutput _ c _ -> do
        -- though this should not happen (this is a Sink, after all...)
        -- run the finalizer and combine the result anyways
        lift c
        step acc xs ys

routeToSinksBy
  :: (Monad m, Ord a)
  => (i -> a)
  -> Map a (Sink i m r)
  -> Sink i m (Map a r)
routeToSinksBy getKey sinkMap = do
    sinkMap' <- lift $ mapM runForward sinkMap
    let (res, sinkMap'') = Map.mapAccumWithKey collectResults Map.empty sinkMap'
    go res $ Map.mapMaybe id sinkMap''
  where go results sinks = do
          mval <- await
          case mval of
            Nothing -> Map.foldlWithKey finalizeSink (return results) sinks
            Just val ->
              let key = getKey val
              in case Map.lookup key sinks of
                  Nothing -> go results sinks -- Ignore inputs for which there are no sinks
                  Just (NeedInput i _) -> cont key (i val)
                  Just _ -> fail "the sink map should only contain sinks in the NeedInput state"
          where cont key snk@(NeedInput _ _) = go results $ Map.insert key snk sinks
                cont key snk@(PipeM _) = lift (runForward snk) >>= cont key
                cont key (Done res) = go (Map.insert key res results) $ Map.delete key sinks
                cont _ (HaveOutput _ _ _) = fail "sinks should not yield values"

        finalizeSink mresults key (NeedInput _ c) = do
          results <- mresults
          res <- closeSink (c ())
          return $! Map.insert key res results
        finalizeSink _ _ _ = fail "the sink map should only contain sinks in the NeedInput state"

        closeSink (NeedInput _ _) = fail "The sink finalizer requested more input"
        closeSink snk@(PipeM _) = lift (runForward snk) >>= closeSink
        closeSink (Done r) = return r
        closeSink (HaveOutput _ _ _) = fail "sinks should not yield values"

        collectResults results _ snk@(NeedInput _ _) = (results, Just snk)
        collectResults results key (Done r) = (Map.insert key r results, Nothing)
        collectResults _ _ (PipeM _) = error "runForward should have moved all sinks out of the PipeM state"
        collectResults _ _ (HaveOutput _ _ _) = error "sinks should not yield values"

routeToSinksBy_
  :: (Monad m, Ord a)
  => (i -> a)
  -> Map a (Sink i m ())
  -> Sink i m ()
routeToSinksBy_ getKey sinkMap = do
    sinkMap' <- lift $ mapM runForward sinkMap
    go $ Map.filter needsInput sinkMap'
  where go sinks = do
          mval <- await
          case mval of
            Nothing -> mapM_ finalizeSink $ Map.elems sinks
            Just val ->
              let key = getKey val
              in case Map.lookup key sinks of
                  Nothing -> go sinks -- Ignore inputs for which there are no sinks
                  Just (NeedInput i _) -> cont key (i val)
                  Just _ -> fail "the sink map should only contain sinks in the NeedInput state"
          where cont key snk@(NeedInput _ _) = go $ Map.insert key snk sinks
                cont key snk@(PipeM _) = lift (runForward snk) >>= cont key
                cont key (Done _) = go $ Map.delete key sinks
                cont _ (HaveOutput _ _ _) = fail "sinks should not yield values"

        finalizeSink (NeedInput _ c) = closeSink (c ())
        finalizeSink _ = fail "the sink map should only contain sinks in the NeedInput state"

        closeSink (NeedInput _ _) = fail "The sink finalizer requested more input"
        closeSink snk@(PipeM _) = lift (runForward snk) >>= closeSink
        closeSink (Done _) = return ()
        closeSink (HaveOutput _ _ _) = fail "sinks should not yield values"

        needsInput (NeedInput _ _) = True
        needsInput (Done _) = False
        needsInput (PipeM _) = error "runForward should have moved all sinks out of the PipeM state"
        needsInput (HaveOutput _ _ _) = error "sinks should not yield values"

chunk :: Monad m => Int -> GInfConduit B.ByteString m B.ByteString
chunk limit = go 0 [] where
  go len xs = do
    mval <- awaitE
    case mval of
      -- terminal conditions
      Left val
        | not . null $ xs -> do
            yield $! B.concat (reverse xs)
            return val

        | otherwise ->
            return val

      -- data has been received...
      Right val
        -- if we have exceeded the size limit, concat the chunks.
        -- this may lead to chunks well over the limit but
        -- in practice it will be ok (and has simpler code)
        | len' > limit -> do
            yield $! B.concat (reverse xs')
            go 0 []

        -- haven't reached the size limit yet, recurse
        | otherwise ->
            go len' xs'

       where
        len' = B.length val + len
        xs'  = val:xs

sourceFromTMVar :: MonadIO m => TMVar (Maybe a) -> GSource m a
sourceFromTMVar var = do
  val <- liftIO . atomically $ takeTMVar var
  case val of
    Just x -> yield x >> sourceFromTMVar var
    Nothing -> return ()

sourceFromTBQueue :: MonadIO m => TBQueue (Maybe a) -> GSource m a
sourceFromTBQueue chan = do
  val <- liftIO . atomically $ readTBQueue chan
  case val of
    Just x -> yield x >> sourceFromTBQueue chan
    Nothing -> return ()

sourceFromTChan :: MonadIO m => TChan (Maybe a) -> GSource m a
sourceFromTChan chan = do
  val <- liftIO . atomically $ readTChan chan
  case val of
    Just x -> yield x >> sourceFromTChan chan
    Nothing -> return ()
{-
sourceFromTQueue :: MonadIO m => TQueue (Maybe a) -> Source m a
sourceFromTQueue queue = do
  val <- liftIO . atomically $ readTQueue queue
  case val of
    Just x -> yield x >> sourceFromTQueue queue
    Nothing -> return () -}

data MergeAsync a
  = MergeValue a
  | MergeComplete
  | MergeException SomeException

-- | This is defined in base-4.6 and can be removed when we upgrade
forkFinally :: IO a -> (Either SomeException a -> IO ()) -> IO ThreadId
forkFinally action and_then =
   mask $ \restore ->
     forkIO $ try (restore action) >>= and_then

mergeSourcesAsync
  :: MonadResource m
  => [Source (ResourceT IO) a]
  -> GSource m a
mergeSourcesAsync sources = do
  ts <- liftIO newTChanIO
  exs <- liftIO newTChanIO

  let forkPumpThread source = do
        -- dump a source into a TChan as data becomes available
        done <- newEmptyTMVarIO

        let body = runResourceT $ source $$ Cl.mapM_ step
            step val = liftIO . atomically $ writeTChan ts val
            complete (Right _) = atomically $ putTMVar done ()
            complete (Left ex) = atomically $ do
              writeTChan exs ex
              putTMVar done ()

        tid <- forkFinally body complete

        return (done, tid)

  -- allocate the pump threads first
  ys <- forM sources $ \ source -> do
    (key, (done, _)) <- lift $ allocate (forkPumpThread source) (\ (_, tid) -> killThread tid)
    return (key, done)

  -- then start processing their results
  forM_ ys $ \ (key, done) ->
    -- keep iterating until 'done' has a value, then switch back to the outer forM_ loop
    let loop = do
          mval <- liftIO . atomically $
                MergeValue         <$> readTChan ts
            <|> MergeException     <$> readTChan exs
            <|> pure MergeComplete <*  takeTMVar done

          case mval of
            MergeValue val -> yield val >> loop
            MergeComplete -> lift $ release key
            MergeException (SomeException e) -> liftIO . throwIO $ e

    in loop

groupBy :: (Monad m, Ord k) => (a -> k) -> (b -> b -> b) -> (a -> b) -> GSink a m (Map k b)
groupBy keySelector combiner valueSelector = Cl.fold step Map.empty where
  step acc el = Map.insertWith combiner (keySelector el) (valueSelector el) acc

flatten :: (Fold.Foldable t, Monad m) => GInfConduit (t a) m a
flatten = do
  val <- C.awaitE
  case val of
    Right x  -> Fold.mapM_ C.yield x >> flatten
    Left val -> return val

unlines :: (IsString a, Monad m) => GInfConduit a m a
unlines = go where
  go = do
    mline <- awaitE
    case mline of
      Right line -> yield line >> yield "\n" >> go
      Left val   -> return val
