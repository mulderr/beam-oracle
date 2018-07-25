{-# language
    FlexibleContexts
  , FlexibleInstances
  , GeneralizedNewtypeDeriving
  , MultiParamTypeClasses
  , OverloadedStrings
  , ScopedTypeVariables
  , TypeFamilies
  , TypeApplications
#-}
module Database.Beam.Oracle.Connection
  ( Oracle(..)
  , Ora(..)

  , runBeamOracle, runBeamOracleDebug
  , rowRep

  , OraCommandSyntax(..)
  , OraSelectSyntax(..), OraInsertSyntax(..)
  , OraUpdateSyntax(..), OraDeleteSyntax(..)
  , OraExpressionSyntax(..)
  ) where

import           Control.Applicative.Free
import           Control.Exception
import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Free.Church
import           Data.ByteString.Builder
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as BL
import           Data.Int
import qualified Data.DList as DL
import           Data.Proxy (Proxy (..))
import           Data.Scientific (Scientific)
import qualified Data.Text as T
import           Data.Time (LocalTime)
import           Data.Word
import qualified Database.Odpi as Odpi
import           Database.Odpi.FromField

import           Database.Beam.Oracle.Syntax
import           Database.Beam.Backend.SQL


-- | The Oracle backend type, used to parametrize 'MonadBeam'. The
-- corresponding query monad is 'Ora'.
data Oracle = Oracle

instance BeamBackend Oracle where
  type BackendFromField Oracle = Odpi.FromField

instance Odpi.FromField SqlNull where
  fromField (Odpi.NativeNull _) = pure SqlNull
  fromField v = Odpi.convError "SqlNull" v

newtype Ora a = Ora { runOra :: ReaderT (String -> IO (), Odpi.Connection) IO a }
  deriving (Functor, Applicative, Monad, MonadIO)

data NotEnoughColumns
  = NotEnoughColumns
  { _errColCount :: Int
  } deriving Show

instance Exception NotEnoughColumns where
  displayException (NotEnoughColumns colCnt) =
    unwords [ "Not enough columns while reading row. Only have"
            , show colCnt, "column(s)" ]

runBeamOracleDebug :: (String -> IO ()) -> Odpi.Connection -> Ora a -> IO a
runBeamOracleDebug logger conn (Ora a) =
  runReaderT a (logger, conn)

runBeamOracle :: Odpi.Connection -> Ora a -> IO a
runBeamOracle = runBeamOracleDebug (\_ -> pure ())

rowRep :: FromBackendRowA Oracle a -> [Maybe Odpi.NativeTypeNum]
rowRep (Pure _) = []
rowRep (Ap (ParseOneField (_ :: (Maybe x -> f))) a) = (Odpi.nativeTypeFor (Proxy @x)) : rowRep a
rowRep (Ap (PeekField (_ :: (Maybe x -> f))) a) = rowRep a

defineValuesForRow :: forall a. FromBackendRow Oracle a => Proxy a -> Odpi.Statement -> IO ()
defineValuesForRow p s = do
  let fbr = fromBackendRow :: FromBackendRowA Oracle (Maybe a)
  n <- Odpi.stmtGetNumQueryColumns s
  -- putStrLn $ "query columns: " ++ show n
  -- putStrLn $ "values needed: " ++ show (valuesNeeded fbr)
  -- putStrLn $ "rowRep: " ++ show (rowRep fbr)
  mapM_ f $ zip [1..n] (rowRep fbr)
  where
    f :: (Word32, Maybe Odpi.NativeTypeNum) -> IO ()
    f (i, mty) =
      case mty of
        Nothing -> pure ()
        Just ty -> Odpi.defineValueForTy s i ty

instance MonadBeam OraCommandSyntax Oracle Odpi.Connection Ora where
  withDatabase = runBeamOracle
  withDatabaseDebug = runBeamOracleDebug

  runReturningMany (OraCommandSyntax _ (OraSyntax cmd vals)) (consume :: Ora (Maybe x) -> Ora a) = do
    (logger, conn) <- Ora ask
    let cmdStr = BL.toStrict $ toLazyByteString $ withPlaceholders cmd
    liftIO $ do
      logger (B8.unpack cmdStr ++ ";\n-- With values: " ++ show (DL.toList vals))
      Odpi.withStatement conn False cmdStr $ \st -> do
        bindValues st vals
        ncol <- Odpi.stmtExecute st Odpi.ModeExecDefault
        defineValuesForRow (Proxy @x) st
        runReaderT (runOra (consume $ nextRow st ncol)) (logger, conn)
    where
      bindValues :: Odpi.Statement -> DL.DList Odpi.NativeValue -> IO ()
      bindValues st vs = mapM_ (bindOne st) $ zip [1..] (DL.toList vs)

      bindOne st (pos, v) = Odpi.stmtBindValueByPos st pos v

-- action that fetches the next row passed to consume
nextRow :: forall x. FromBackendRow Oracle x => Odpi.Statement -> Word32 -> Ora (Maybe x)
nextRow st ncol = Ora $ liftIO $ do
  mPageOffset <- Odpi.stmtFetch st
  case mPageOffset of
    Nothing -> do
      pure Nothing
    Just _ -> do
      fields <- mapM (\n -> Odpi.stmtGetQueryValue st n) [1..ncol]
      evalStateT (runAp step fromBackendRow) fields
  where
    step :: forall a. FromBackendRowF Oracle a -> StateT [Odpi.NativeValue] IO a
    step (ParseOneField (next :: Maybe res -> a)) = do
      df:fs <- get
      put fs
      case Odpi.isNativeNull df of
        True -> pure $ next Nothing
        False -> do
          d <- liftIO $ fromField df
          pure $ next $ Just d
    step (PeekField _) = error "TODO: need PeekField after all"

instance FromBackendRow Oracle SqlNull
instance FromBackendRow Oracle Bool
instance FromBackendRow Oracle Word
instance FromBackendRow Oracle Word16
instance FromBackendRow Oracle Word32
instance FromBackendRow Oracle Word64
instance FromBackendRow Oracle Int
instance FromBackendRow Oracle Int16
instance FromBackendRow Oracle Int32
instance FromBackendRow Oracle Int64
instance FromBackendRow Oracle Float
instance FromBackendRow Oracle Double
instance FromBackendRow Oracle Scientific
instance FromBackendRow Oracle Char
instance FromBackendRow Oracle B.ByteString
instance FromBackendRow Oracle T.Text
instance FromBackendRow Oracle LocalTime
