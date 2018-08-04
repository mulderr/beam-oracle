{-# language
    MultiParamTypeClasses
  , OverloadedStrings
  , ScopedTypeVariables
  , TypeApplications
#-}
module Test where

import Test.Hspec
import Test.QuickCheck
import Test.QuickCheck.Monadic

import Data.Int
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B8
import Data.Scientific
import Data.Text (Text)
import Data.Tuple.Only
import Database.Odpi hiding (rowRep)
import System.Environment

import Database.Beam
import Database.Beam.Oracle

import Chinook.Schema

------------------------------------------------------------------------------

type BackendM = Ora

conf :: OdpiConf
conf = OdpiConf
  { _odpiConf_username = "chinook"
  , _odpiConf_password = "p4ssw0rd"
  , _odpiConf_connstr  = "192.168.56.101/XE"
  }

main :: IO ()
main = do
  setEnv "NLS_LANG" "AMERICAN_AMERICA.AL32UTF8"
  withConn $ hspec . spec

spec :: Connection -> Spec
spec conn = do
  describe "null handling for individual fields:" $ do
    it "a from not-null works" $ do
      r1 <- runQ $ q0 1
      r2 <- rawQ "SELECT t.NAME FROM ARTIST t WHERE t.ARTISTID = 1"
      r1 `shouldBe` Just (fromOnly $ head r2)
    it "Maybe a from not-null is always Just" $ do
      r1 <- runQ nullTest1
      r2 <- rawQ "select t.CUSTOMERID, t.EMAIL from CUSTOMER t"
      r1 `shouldBe` (fmap (fmap Just) r2)
    it "a from null throws an exception" $ do
      (runQ nullTest2) `shouldThrow` anyException
    it "Maybe a from null works" $ do
      r1 <- runQ nullTest3
      r2 <- rawQ "select t.CUSTOMERID, t.COMPANY from CUSTOMER t"
      r1 `shouldBe` r2

  describe "simple queries with nullable fields:" $ do
    it "1" $ do
      r1 <- runQ q2
      r2 <- rawQ "SELECT t.EMPLOYEEID, t.TITLE FROM EMPLOYEE t"
      r1 `shouldBe` r2
    it "2" $ do
      r1 <- runQ q3
      r2 <- rawQ "SELECT t.CUSTOMERID, t.COMPANY FROM CUSTOMER t"
      r1 `shouldBe` r2
    it "3" $ do
      r1 <- runQ q4
      r2 <- rawQ "SELECT t.CUSTOMERID, t.COMPANY, t.EMAIL FROM CUSTOMER t"
      r1 `shouldBe` r2
    it "4" $ do
      _ <- runQ q5
      1 `shouldBe` (1 :: Int)

  describe "data types" $ do
    describe "text" $ do
      it "handles UTF-8" $ do
        r <- runQ $ q0 146
        r `shouldBe` (Just "Titãs")
      it "handles null" $ do
        r <- fmap (fromOnly . head) $ rawQ $ "select null from dual"
        r `shouldBe` (Nothing :: Maybe Text)

  describe "inner join" $ do
    it "simple" $ do
      r1 <- runQ $ runSelectReturningList $ select $ do
        a <- all_ $ artist chinookDb
        al <- join_ (album chinookDb) $ \r -> albumArtist r ==. pk a
        pure (artistId a, albumId al)
      r2 <- rawQ "SELECT t0.ARTISTID, t1.ALBUMID FROM ARTIST t0 JOIN ALBUM t1 on (t1.ARTISTID = t0.ARTISTID)"
      r1 `shouldBe` r2
    it "with table types" $ do
      r <- runQ joinInner1
      (length r) `shouldBe` (347 :: Int)

  describe "left outer join:" $ do
    it "simple" $ do
      r1 <- runQ $ runSelectReturningList $ select $ do
        a <- all_ $ artist chinookDb
        al <- leftJoin_ (all_ $ album chinookDb) $ \r -> albumArtist r ==. pk a
        pure (artistId a, albumId al)
      r2 <- rawQ "SELECT t0.ARTISTID, t1.ALBUMID FROM ARTIST t0 LEFT JOIN ALBUM t1 on (t1.ARTISTID = t0.ARTISTID)"
      r1 `shouldBe` r2
    it "with table types" $ do
      r1 <- rawQ $ B8.unwords [ "SELECT t0.ARTISTID, t0.NAME,"
                              , "t1.ALBUMID, t1.TITLE, t1.ARTISTID"
                              , "FROM ARTIST t0 LEFT JOIN ALBUM t1 ON (t1.ARTISTID) = (t0.ARTISTID)"
                              ]
      length (r1 :: [(Pk32, Text, Maybe Pk32, Maybe Text, Maybe Pk32)]) `shouldBe` (418 :: Int)
      let r1' = fmap (\(a, b, mc, md, me) ->
                        ( Artist a b
                        , Album <$> mc <*> md <*> fmap ArtistId me
                        )
                     ) r1
      r2 <- runQ joinLeft1
      (length r2) `shouldBe` (418 :: Int)
      r2 `shouldBe` r1'

  describe "ordering" $ do
    it "works asc" $ do
      r <- runQ orderAsc
      (head r) `shouldBe` "A Cor Do Som"
    it "works desc" $ do
      r <- runQ orderDesc
      (head r) `shouldBe` "Zeca Pagodinho"

  describe "aggregates" $ do
    it "count" $ do
      r <- runQ aggCount1
      r `shouldBe` (Just 412)
    it "sum" $ do
      r <- runQ aggSum
      r `shouldBe` (Just 2328.6)
    it "min" $ do
      r <- runQ aggMin
      r `shouldBe` (Just 0.99)
    it "max" $ do
      r <- runQ aggMax
      r `shouldBe` (Just 25.86)
    it "avg, does not loose precision" $ do
      -- r1 <- runQ aggAvg
      -- r1 `shouldBe` (Just 393599.2121039109334855837853268626891236)
      r2 <- fmap (fromOnly . head) $ rawQ $ "select avg(milliseconds) from track"
      r2 `shouldBe` (393599.2121039109334855837853268626891236 :: Scientific)

  describe "aggregates with group by" $ do
    -- it "count" $ do
    --   r <- runQ agg2Count1
    --   r `shouldBe` [(CustomerId 44, 7), (CustomerId 59, 6)]
    it "sum" $ do
      r <- runQ agg2Sum
      r `shouldBe` [(CustomerId 44, 41.62), (CustomerId 59, 36.64)]

  describe "limit" $ do
    it "ruturns empty list when 0" $ do
      r <- runQ $ limit1 0
      r `shouldBe` []
    it "is a noop when >= number of rows" $ do
      r <- runQ $ limit1 300
      (length r) `shouldBe` 275
    it "passes unit tests" $ do
      r1 <- runQ $ limit1 1
      r1 `shouldBe` ["A Cor Do Som"]
      r2 <- runQ $ limit1 2
      r2 `shouldBe` ["A Cor Do Som", "AC/DC"]
    it "passes property tests" $ property $ \l -> l > 0 && l < 275 ==> monadicIO $ do
        (r1, r2) <- run $ do
          r1 <- runQ' conn $ limit1 l
          r2 <- rawQ' conn $ B8.unwords
            [ "select * from ("
            ,   "select name from artist order by name"
            , ") where rownum <= ", B8.pack (show l)
            ]
          pure (r1, fmap fromOnly r2)
        assert $ r1 == r2

  describe "offset" $ do
    it "is a noop when 0" $ do
      r <- runQ $ offset1 0
      (length r) `shouldBe` 275
    it "returns empty list when >= number of rows" $ do
      r <- runQ $ offset1 300
      r `shouldBe` []
    it "passes unit tests" $ do
      r <- runQ $ offset1 274
      r `shouldBe` ["Zeca Pagodinho"]
    it "passes property tests" $ property $ \o -> o >= 0 && o < 275 ==> monadicIO $ do
        (r1, r2) <- run $ do
          r1 <- runQ' conn $ offset1 o
          r2 <- rawQ' conn $ B8.unwords
            [ "select name from ("
            ,   "select a.name as name, rownum as rnum"
            ,   "from (select name from artist order by name) a"
            , ") where rnum >", B8.pack (show o)
            ]
          pure (r1, fmap fromOnly r2)
        assert $ r1 == r2

  describe "offset and limit" $ do
    it "work well together" $ do
      r1 <- runQ $ offsetLimit1 0 0
      r1 `shouldBe` []
      r2 <- runQ $ offsetLimit1 1 1
      r2 `shouldBe` ["AC/DC"]
      r3 <- runQ $ offsetLimit1 2 2
      r3 `shouldBe` ["Aaron Copland & London Symphony Orchestra", "Aaron Goldberg"]

  describe "filtering" $ do
    it "handles simple injection" $ do
      r <- runQ $ runSelectReturningList $ select $ do
        c <- all_ $ customer chinookDb
        guard_ $ customerFirstName c ==. val_ "Bobby Tables') or 1=1 -- "
        pure c
      (length r) `shouldBe` 0

withConn :: (Connection -> IO a) -> IO a
withConn f =
  withContext $ \cxt ->
    withConnection cxt conf f

runQ :: Ora a -> IO a
runQ q = withConn $ \conn -> runQ' conn q

runQ' :: Connection -> Ora a -> IO a
runQ' =
  runBeamOracle
  -- runBeamOracleDebug putStrLn

rawQ :: FromRow a => ByteString -> IO [a]
rawQ q = withConn $ \conn -> rawQ' conn q

rawQ' :: FromRow a => Connection -> ByteString -> IO [a]
rawQ' = querySimple

q0 :: Pk32 -> BackendM (Maybe Text)
q0 aid = runSelectReturningOne $ select $ do
  a <- all_ $ artist chinookDb
  guard_ $ pk a ==. val_ (ArtistId aid)
  pure $ artistName a

q1 :: BackendM [(Pk32, Text)]
q1 = runSelectReturningList $ select $ do
  a <- all_ $ artist chinookDb
  pure (artistId a, artistName a)

q2 :: BackendM [(Pk32, Maybe Text)]
q2 = runSelectReturningList $ select $ do
  a <- all_ $ employee chinookDb
  pure (employeeId a, employeeTitle a)

q3 :: BackendM [(Pk32, Maybe Text)]
q3 = runSelectReturningList $ select $ do
  a <- all_ $ customer chinookDb
  pure (customerId a, customerCompany a)

q4 :: BackendM [(Pk32, Maybe Text, Text)]
q4 = runSelectReturningList $ select $ do
  a <- all_ $ customer chinookDb
  pure (customerId a, customerCompany a, customerEmail a)

q5 :: BackendM [(Customer, Maybe Employee)]
q5 = runSelectReturningList $ select $ do
  c <- all_ (customer chinookDb)
  e <- leftJoin_ (all_ (employee chinookDb)) (\e -> addressCity (employeeAddress e) ==. addressCity (customerAddress c))
  pure (c, e)

nullTest1 :: BackendM [(Pk32, Maybe Text)]
nullTest1 = runSelectReturningList $ select $ do
  a <- all_ $ customerBroken1 chinookDb
  pure (customerBroken1Id a, customerBroken1Email a)

nullTest2 :: BackendM [(Pk32, Text)]
nullTest2 = runSelectReturningList $ select $ do
  a <- all_ $ customerBroken2 chinookDb
  pure (customerBroken2Id a, customerBroken2Company a)

nullTest3 :: BackendM [(Pk32, Maybe Text)]
nullTest3 = runSelectReturningList $ select $ do
  a <- all_ $ customer chinookDb
  pure (customerId a, customerCompany a)

joinInner1 :: BackendM [(Artist, Album)]
joinInner1 = runSelectReturningList $ select $ do
  a <- all_ $ artist chinookDb
  al <- join_ (album chinookDb) $ \r -> albumArtist r ==. pk a
  pure (a, al)

joinLeft1 :: BackendM [(Artist, Maybe Album)]
joinLeft1 = runSelectReturningList $ select $ do -- limit_ 3 $ offset_ 400 $ do
  a <- all_ $ artist chinookDb
  al <- leftJoin_ (all_ $ album chinookDb) $ \r -> albumArtist r ==. pk a
  pure (a, al)

orderAsc :: BackendM [Text]
orderAsc = runSelectReturningList $ select $ orderBy_ asc_ $ do
  a <- all_ $ artist chinookDb
  pure $ artistName a

orderDesc :: BackendM [Text]
orderDesc = runSelectReturningList $ select $ orderBy_ desc_ $ do
  a <- all_ $ artist chinookDb
  pure $ artistName a

aggCount1 :: BackendM (Maybe Pk32)
aggCount1 = runSelectReturningOne $ select $
  aggregate_ (\i -> count_ (invoiceId i)) $
  all_ $ invoice chinookDb

aggSum :: BackendM (Maybe Scientific)
aggSum = runSelectReturningOne $ select $
  aggregate_ (\i -> fromMaybe_ 0 $ sum_ (invoiceTotal i)) $
  all_ $ invoice chinookDb

aggMin :: BackendM (Maybe Scientific)
aggMin = runSelectReturningOne $ select $
  aggregate_ (\i -> fromMaybe_ 0 $ min_ (invoiceTotal i)) $
  all_ $ invoice chinookDb

aggMax :: BackendM (Maybe Scientific)
aggMax = runSelectReturningOne $ select $
  aggregate_ (\i -> fromMaybe_ 0 $ max_ (invoiceTotal i)) $
  all_ $ invoice chinookDb

-- TODO: how to get Scientific?
aggAvg :: BackendM (Maybe Int32)
aggAvg = runSelectReturningOne $ select $
  aggregate_ (\t -> fromMaybe_ 0 $ avg_ (trackMilliseconds t)) $
  all_ $ track chinookDb

-- agg2Count1 :: BackendM [(CustomerId, Exactly Int)]
-- agg2Count1 = runSelectReturningList $ select $
--   aggregate_ (\c -> (group_ c, countAll_)) $ do
--     i <- all_ $ invoice chinookDb
--     guard_ $ invoiceCustomer i ==. CustomerId (val_ 44) ||. invoiceCustomer i ==. CustomerId (val_ 59)
--     pure (invoiceCustomer i)

agg2Sum :: BackendM [(CustomerId, Scientific)]
agg2Sum = runSelectReturningList $ select $
  aggregate_ (\i -> (group_ (invoiceCustomer i), fromMaybe_ 0 $ sum_ (invoiceTotal i))) $
  filter_ (\i -> invoiceCustomer i ==. CustomerId (val_ 44)
             ||. invoiceCustomer i ==. CustomerId (val_ 59)) $
  all_ $ invoice chinookDb

limit1 :: Integer -> BackendM [Text]
limit1 l = runSelectReturningList $ select $ limit_ l $ orderBy_ asc_ $ do
  a <- all_ $ artist chinookDb
  pure $ artistName a

offset1 :: Integer -> BackendM [Text]
offset1 o = runSelectReturningList $ select $ offset_ o $ orderBy_ asc_ $ do
  a <- all_ $ artist chinookDb
  pure $ artistName a

offsetLimit1 :: Integer -> Integer -> BackendM [Text]
offsetLimit1 o l = runSelectReturningList $ select $ limit_ l $ offset_ o $ orderBy_ asc_ $ do
  a <- all_ $ artist chinookDb
  pure $ artistName a
