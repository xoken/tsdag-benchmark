module Main where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async
import Control.Monad
import Criterion
import Criterion.Main
import Data.ByteString
import Data.Foldable as F
import Data.Serialize
import Data.Serialize.Put
import Network.Xoken.Node.Data.ThreadSafeDirectedAcyclicGraph as TSDAG
import Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Prelude

sequentialInsert :: Int -> IO ([Int])
sequentialInsert reps = do
    dag <- TSDAG.new 1 0
    Prelude.mapM
        (\x -> do
             if x > 1
                 then coalesce dag x [x - 1]
                 else coalesce dag x [])
        [1 .. reps]
    tsd <- TSH.toList $ topologicalSorted dag
    print ("Sequential:")
    mapM (\(h, x) -> do print (h, F.toList x)) tsd
    verts <- TSH.toList $ vertices dag
    print ("Vertices: ", verts)
    return [] -- $ topologicalSorted dag 

asyncInsert :: Int -> IO ([Int])
asyncInsert reps = do
    dag <- TSDAG.new 1 0
    mapConcurrently -- async
        (\x -> do
             if x > 1
                 then coalesce dag x [x - 1]
                 else coalesce dag x [])
        [1 .. reps]
    tsd <- TSH.toList $ topologicalSorted dag
    print ("Async:")
    mapM (\(h, x) -> do print (h, F.toList x)) tsd
    verts <- TSH.toList $ vertices dag
    print ("------------VERTICES--------------")
    print ("Vertices: ", verts)
    print ("----------CONSOLIDATED-ONE-----------")
    consolidate dag
    tsd <- TSH.toList $ topologicalSorted dag
    mapM (\(h, x) -> do print (h, F.toList x)) tsd
    print ("----------CONSOLIDATED-TWO-----------")
    consolidate dag
    tsd <- TSH.toList $ topologicalSorted dag
    mapM (\(h, x) -> do print (h, F.toList x)) tsd
    return [] -- $ topologicalSorted dag 

test :: IO (Bool)
test
    --seq <- sequentialInsert 200
 = do
    asy <- asyncInsert 2000
    return False -- $ seq == asy

main :: IO ()
main = do
    res <- test
    when (not res) $ error "result mismatch b/w sequentialInsert and asyncInsert"
    defaultMain workload
  where
    workload =
        (flip Prelude.concatMap) [1 .. 3] $ \x ->
            let reps = 10 * (10 ^ x)
             in [ bench (show reps <> " sequential insert ") $ whnf sequentialInsert reps
                , bench (show reps <> " async insert ") $ whnf asyncInsert reps
                ]
