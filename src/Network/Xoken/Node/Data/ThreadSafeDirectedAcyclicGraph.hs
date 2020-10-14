{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE BangPatterns #-}

module Network.Xoken.Node.Data.ThreadSafeDirectedAcyclicGraph
    ( TSDirectedAcyclicGraph(..)
    , new
    , coalesce
    ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async.Lifted as LA (async, race)
import Control.Concurrent.MVar
import Control.Exception
import qualified Control.Exception.Extra as EX
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad.IO.Class
import Control.Monad.STM
import Data.Function
import qualified Data.HashTable.IO as H
import Data.Hashable
import Data.Int
import qualified Data.List as L
import Data.Sequence as SQ
import Data.Set as ST
import Data.Text as T
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Numeric as N

data DAGException =
    InsertTimeoutException
    deriving (Show)

instance Exception DAGException

data TSDirectedAcyclicGraph v =
    TSDirectedAcyclicGraph
        { vertices :: !(TSH.TSHashTable v v) -- mapping of vertex to head-of-Sequence
        , topologicalSorted :: !(TSH.TSHashTable v (Seq v)) -- key : value :: head : sequence
        , dependents :: !(TSH.TSHashTable v (MVar v)) -- 
        , baseVertex :: !(v)
        , lock :: MVar ()
        }

new :: (Eq v, Hashable v, Ord v, Show v) => Int16 -> v -> IO (TSDirectedAcyclicGraph v)
new vsize def = do
    vertices <- TSH.new vsize
    topoSorted <- TSH.new 1
    dep <- TSH.new 1
    TSH.insert topoSorted def SQ.empty
    lock <- newMVar ()
    return $ TSDirectedAcyclicGraph vertices topoSorted dep def lock

coalesce :: (Eq v, Hashable v, Ord v, Show v) => TSDirectedAcyclicGraph v -> v -> [v] -> IO ()
coalesce dag vt edges
    -- print (vt)
 = do
    takeMVar (lock dag)
    vals <-
        mapM
            (\dep -> do
                 res <- TSH.lookup (vertices dag) dep
                 case res of
                     Just indx -> do
                         fix -- do multi level recursive lookup
                             (\f n -> do
                                  res2 <- TSH.lookup (vertices dag) n
                                  case res2 of
                                      Just ix ->
                                          if n == ix
                                              then return ix
                                              else f (ix)
                                      Nothing -> return n)
                             indx
                         --return indx 
                     Nothing -> return dep)
            edges
    putMVar (lock dag) ()
    if L.null vals
        then do
            baseSeq <- TSH.lookup (topologicalSorted dag) (baseVertex dag)
            case baseSeq of
                Just bsq -> do
                    TSH.insert (topologicalSorted dag) (baseVertex dag) (bsq |> vt) -- 
                    TSH.insert (vertices dag) vt (baseVertex dag) --
                Nothing -> print ("baseSeq not found")
        else do
            let head = vals !! 0
            if L.all (\x -> x == head) vals -- if all are same
                then do
                    seq <- TSH.lookup (topologicalSorted dag) (head)
                    case seq of
                        Just sq -> do
                            etry <- TSH.lookup (vertices dag) vt
                            TSH.insert (vertices dag) vt head
                            case etry of
                                Just x -> do
                                    hseq <- TSH.lookup (topologicalSorted dag) x
                                    case hseq of
                                        Just hs -> do
                                            TSH.insert (topologicalSorted dag) head (sq <> hs)
                                            TSH.delete (topologicalSorted dag) x
                                        Nothing -> do
                                            TSH.insert (topologicalSorted dag) head (sq |> vt)
                                Nothing -> do
                                    TSH.insert (topologicalSorted dag) head (sq |> vt)
                            event <- TSH.lookup (dependents dag) vt
                            case event of
                                Just ev -> liftIO $ putMVar ev head -- value in MVar, vt or head?
                                Nothing -> return ()
                            -- if head == (baseVertex dag)
                            --     then return ()
                            --     else coalesce dag vt vals
                        Nothing -> do
                            TSH.insert (topologicalSorted dag) vt (SQ.singleton vt) -- 
                            TSH.insert (vertices dag) vt vt -- needed?
                            -- added below newly
                            par <-
                                mapM
                                    (\dep -> do
                                         vrtx <- TSH.lookup (vertices dag) dep
                                         case vrtx of
                                             Just vx -> do
                                                 takeMVar (lock dag)
                                                 z <-
                                                     fix -- do multi level recursive lookup
                                                         (\f n -> do
                                                              res2 <- TSH.lookup (vertices dag) n
                                                              case res2 of
                                                                  Just ix ->
                                                                      if n == ix
                                                                          then return ix
                                                                          else f (ix)
                                                                  Nothing -> return n)
                                                         vx
                                                 putMVar (lock dag) ()
                                                 return z
                                             Nothing -> do
                                                 event <-
                                                     TSH.mutateIO
                                                         (dependents dag)
                                                         dep
                                                         (\x ->
                                                              case x of
                                                                  Just e -> return (x, e)
                                                                  Nothing -> do
                                                                      em <- newEmptyMVar
                                                                      return (Just em, em))
                                                    --
                                                 ores <-
                                                     LA.race
                                                         (liftIO $ readMVar event)
                                                         (liftIO $ threadDelay (10 * 1000000))
                                                 case ores of
                                                     Right () -> throw InsertTimeoutException
                                                     Left res -> do
                                                         print ("event!", res)
                                                         return res)
                                    vals
                            let uniq = ST.toList $ ST.fromList par
                            coalesce dag vt uniq
                else do
                    TSH.insert (topologicalSorted dag) vt (SQ.singleton vt) -- needed?
                    TSH.insert (vertices dag) vt vt -- needed?
                    par <-
                        mapM
                            (\dep -> do
                                 event <-
                                     TSH.mutateIO
                                         (dependents dag)
                                         dep
                                         (\x ->
                                              case x of
                                                  Just e -> return (x, e)
                                                  Nothing -> do
                                                      em <- newEmptyMVar
                                                      return (Just em, em))
                                 --
                                 ores <- LA.race (liftIO $ readMVar event) (liftIO $ threadDelay (10 * 1000000))
                                 case ores of
                                     Right () -> throw InsertTimeoutException
                                     Left res -> do
                                         print ("event22!", res)
                                         return res)
                            vals
                    let uniq = ST.toList $ ST.fromList par
                    coalesce dag vt uniq
    verts <- TSH.toList $ vertices dag
    print ("Vertices: ", verts)
--
--
--
--            
