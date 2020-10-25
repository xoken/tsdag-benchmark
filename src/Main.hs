module Main where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async
import Control.Concurrent.MVar
import Control.Monad
import Criterion
import Criterion.Main
import Data.ByteString
import Data.Foldable as F
import Data.Serialize
import Data.Serialize.Put
import Data.Word
import Network.Xoken.Node.Data.ThreadSafeDirectedAcyclicGraph as TSDAG
import Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Prelude

sequentialInsert :: Int -> IO ([Int])
sequentialInsert reps = do
    dag <- TSDAG.new 0 (0 :: Word64) 100 5
    Prelude.mapM
        (\x -> do
             if x > 1
                 then coalesce dag x [x - 1] (fromIntegral x) accumulate
                 else coalesce dag x [] (fromIntegral x) accumulate)
        [1 .. reps]
    tsd <- TSH.toList $ topologicalSorted dag
    print ("Sequential:")
    mapM (\(h, x) -> do print (h, F.toList x)) tsd
    verts <- TSH.toList $ vertices dag
    print ("Vertices: ", verts)
    print ("Sync:")
    mapM (\(h, x) -> do print (h, F.toList x)) tsd
    return [] -- $ topologicalSorted dag 
  where
    accumulate = (\x a -> a + x)

getList :: Int -> [Int]
getList x
    -- | x `mod` 1000 == 0 = [x - 1]
    -- | x > 8 = [x - 1, quot x 2, (quot x 2) - 1]
    | x > 4 = [x - 1, quot x 2]
    | x > 1 = [x - 1]
    | otherwise = []

asyncInsert :: Int -> IO ([Int])
asyncInsert reps = do
    dag <- TSDAG.new 0 (0 :: Word64) 100 5
    mapM
        (\(start, end) -> do
             mapConcurrently -- async
                 (\x -> do coalesce dag x (getList x) (fromIntegral x) accumulate)
                 [start .. end]
             print (start, end))
        [ (1, 200)
        -- , (1000, 2000)
        -- , (2000, 3000)
        -- , (3000, 4000)
        -- , (4000, 5000)
        -- , (5000, 6000)
        -- , (6000, 7000)
        -- , (7000, 8000)
        -- , (8000, 9000)
        -- , (9000, 10000)
        -- , (10000, 11000)
        -- , (11000, 12000)
        -- , (12000, 13000)
        -- , (13000, 14000)
        -- , (14000, 15000)
        -- , (15000, 16000)
        -- , (16000, 17000)
        -- , (17000, 18000)
        -- , (18000, 19000)
        -- , (19000, 20000)
        ]
    print ("####")
    tsd <- TSH.toList $ topologicalSorted dag
    print ("Async:")
    mapM (\(h, x) -> do print (h, F.toList x)) tsd
    verts <- TSH.toList $ vertices dag
    print ("-----------------VERTICES-----------------")
    print ("Vertices: ", verts)
    print ("----------CONSOLIDATED-PASS-ONE-----------")
    consolidate dag accumulate
    tsd <- TSH.toList $ topologicalSorted dag
    mapM (\(h, x) -> do print (h, F.toList x)) tsd
    print ("----------CONSOLIDATED-PASS-TWO-----------")
    consolidate dag accumulate
    tsd <- TSH.toList $ topologicalSorted dag
    mapM (\(h, x) -> do print (h, F.toList x)) tsd
    return [] -- $ topologicalSorted dag 
  where
    accumulate = (\x a -> a + x)

test :: IO (Bool)
test = do
    seq <- sequentialInsert 200
    asy <- asyncInsert 20000
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
