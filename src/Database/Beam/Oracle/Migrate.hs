{-# language
    CPP
  , DataKinds
  , DeriveGeneric
  , FlexibleInstances
  , MultiParamTypeClasses
  , OverloadedStrings
  , RankNTypes
  , RecordWildCards
  , ScopedTypeVariables
  , TypeApplications
  , TypeSynonymInstances
  , UndecidableInstances
#-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

-- | Migrations support for beam-oracle. See "Database.Beam.Migrate" for more
-- information on beam migrations.
module Database.Beam.Oracle.Migrate
  ( migrationBackend
  , oraPredConverter
  , getDbConstraints
  , oraTypeToHs
  , migrateScript
  , writeMigrationScript
  ) where

import           Database.Beam.Backend.SQL
import           Database.Beam.Migrate.Actions (defaultActionProvider)
import           Database.Beam.Migrate.Backend (BeamMigrationBackend (..))
import qualified Database.Beam.Migrate.Backend as Tool
import qualified Database.Beam.Migrate.Checks as Db
--import           Database.Beam.Migrate.SQL.BeamExtensions
import qualified Database.Beam.Migrate.SQL as Db
import qualified Database.Beam.Migrate.Serialization as Db
import qualified Database.Beam.Migrate.Types as Db

import           Database.Beam.Oracle.Connection
import           Database.Beam.Oracle.Syntax

import           Database.Beam.Haskell.Syntax

import qualified Database.Odpi as Odpi

import           Control.Monad
import           Control.Monad.Reader (ask)
import           Control.Monad.IO.Class (liftIO)

import qualified Data.Aeson as AE
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as B8L
import           Data.Monoid (Endo (..), appEndo)
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import           Data.Tuple.Only
import qualified Data.Yaml as Y
import GHC.Generics
#if !MIN_VERSION_base(4, 11, 0)
import           Data.Semigroup
#endif

data ConnParams
  = ConnParams
  { _connUsername :: Text
  , _connPassword :: Text
  , _connConnstr :: Text
  } deriving (Eq, Show, Generic)

customOptions :: AE.Options
customOptions = AE.defaultOptions { AE.fieldLabelModifier = id } -- T.unpack . T.toLower . T.stripPrefix "_conn" . T.pack }
instance Y.ToJSON ConnParams where
  toJSON = AE.genericToJSON customOptions
  toEncoding = AE.genericToEncoding customOptions
instance Y.FromJSON ConnParams where
  parseJSON = AE.genericParseJSON customOptions

-- | Top-level migration backend for use by @beam-migrate@ tools
migrationBackend :: BeamMigrationBackend OraCommandSyntax Oracle Odpi.Connection Ora
migrationBackend = BeamMigrationBackend
  { backendName = "oracle"
  , backendConnStringExplanation = "For beam-oracle the connection string is a YAML map with the following keys:\n"
      ++ "{ username: 'chinook', password: 'p4ssw0rd', connstr: '127.0.0.1/XE' }"
  , backendRenderSteps = BL.concat . migrateScript
  , backendGetDbConstraints = do
      (_, conn) <- Ora ask
      liftIO $ getDbConstraints conn
  , backendPredicateParsers = mconcat
      [ Db.sql92Deserializers
      , Db.sql99DataTypeDeserializers
      , Db.sql2008BigIntDataTypeDeserializers
      , Db.beamCheckDeserializers
      ]
  , backendRenderSyntax = B8L.unpack . (<> ";") . oraRenderSyntaxScript . fromOraCommand
  , backendFileExtension = "oracle.sql"
  , backendConvertToHaskell = oraPredConverter
  , backendActionProvider = defaultActionProvider
  , backendTransact = oraTransact
  }
  where
    oraTransact yamlStr action =
      case Y.decodeEither' $ B8.pack yamlStr of
        Left err -> pure $ Left $ show err
        Right ConnParams{..} -> do
          Odpi.withContext $ \cxt -> do
            Odpi.withConnection' cxt (TE.encodeUtf8 _connUsername)
                                     (TE.encodeUtf8 _connPassword)
                                     (TE.encodeUtf8 _connConnstr) $ \conn -> do
              r <- runBeamOracle conn action
              pure $ Right r

-- | Converts oracle 'DatabasePredicate's to 'DatabasePredicate's in the
-- Haskell syntax. Allows automatic generation of Haskell schemas from oracle
-- constraints.
oraPredConverter :: Tool.HaskellPredicateConverter
oraPredConverter = Tool.sql92HsPredicateConverters @OraColumnSchemaSyntax oraTypeToHs <>
                  Tool.hsPredicateConverter oraHasColumnConstraint
  where
    oraHasColumnConstraint (Db.TableColumnHasConstraint tblNm colNm c
                              :: Db.TableColumnHasConstraint OraColumnSchemaSyntax)
      | c == Db.constraintDefinitionSyntax Nothing Db.notNullConstraintSyntax Nothing =
          Just (Db.SomeDatabasePredicate (Db.TableColumnHasConstraint tblNm colNm (Db.constraintDefinitionSyntax Nothing Db.notNullConstraintSyntax Nothing) :: Db.TableColumnHasConstraint HsColumnSchema))
      | otherwise = Nothing

-- | Turn a 'OraDataTypeSyntax' into the corresponding 'HsDataType'. This is a
-- best effort guess, and may fail on more exotic types. Feel free to send PRs
-- to make this function more robust!
oraTypeToHs :: OraDataTypeSyntax -> Maybe HsDataType
oraTypeToHs (OraDataTypeSyntax tyDescr _ _) =
  case tyDescr of
    --OraDataTypeDescr ty width prec scale

    -- Character datatypes
    OraDataTypeDescr "CHAR" w _ _ -> Just (charType (fromIntegral <$> w) Nothing)
    OraDataTypeDescr "NCHAR" w _ _ -> Just (nationalCharType (fromIntegral <$> w))
    OraDataTypeDescr "VARCHAR" w _ _ -> Just (varCharType (fromIntegral <$> w) Nothing)
    OraDataTypeDescr "VARCHAR2" w _ _ -> Just (varCharType (fromIntegral <$> w) Nothing)
    OraDataTypeDescr "NVARCHAR2" w _ _ -> Just (nationalVarCharType (fromIntegral <$> w))
    OraDataTypeDescr "CLOB" _ _ _ -> Just characterLargeObjectType
    OraDataTypeDescr "NCLOB" _ _ _ -> Just characterLargeObjectType

    -- Numeric datatypes
    OraDataTypeDescr "NUMBER" _ Nothing Nothing -> Just (numericType (Just (38, Nothing)))
    OraDataTypeDescr "NUMBER" _ (Just 5) (Just 0) -> Just smallIntType
    OraDataTypeDescr "NUMBER" _ (Just 10) (Just 0) -> Just intType
    OraDataTypeDescr "NUMBER" _ (Just 19) (Just 0) -> Just bigIntType
    OraDataTypeDescr "NUMBER" _ mp ms -> Just (numericType (fmap (\p -> (p, fmap fromIntegral ms)) mp))
    OraDataTypeDescr "BINARY_FLOAT" _ _ _ -> Just (floatType Nothing)
    OraDataTypeDescr "BINARY_DOUBLE" _ _ _ -> Just doubleType

    -- Date
    OraDataTypeDescr "DATE" _ _ _ -> Just dateType
    OraDataTypeDescr "TIMESTAMP" _ _ _ -> Just (timestampType Nothing False)
    OraDataTypeDescr "TIMESTAMP WITH TIME ZONE" _ _ _ -> Just (timestampType Nothing True)
    OraDataTypeDescr "TIMESTAMP WITH LOCAL TIME ZONE" _ _ _ -> Just (timestampType Nothing True) -- TODO: ???

    -- LOB
    OraDataTypeDescr "BLOB" _ _ _ -> Just binaryLargeObjectType
    OraDataTypeDescr "BFILE" _ _ _ -> Nothing -- TODO: ???

    _ -> Nothing


-- | Turn a series of 'Db.MigrationSteps' into a line-by-line array of
-- 'BL.ByteString's suitable for writing to a script.
migrateScript :: Db.MigrationSteps OraCommandSyntax () a -> [BL.ByteString]
migrateScript steps =
  appEndo (Db.migrateScript renderHeader renderCommand steps) []
  where
    renderHeader nm =
      Endo (("-- " <> BL.fromStrict (TE.encodeUtf8 nm) <> "\n"):)
    renderCommand command =
      Endo ((oraRenderSyntaxScript (fromOraCommand command) <> ";\n"):)

-- * Create constraints from a connection

getDbConstraints :: Odpi.Connection -> IO [ Db.SomeDatabasePredicate ]
getDbConstraints conn = do
  tbls :: [Text] <- fmap (fmap fromOnly) $ Odpi.querySimple conn (B8.unlines
    [ "select table_name"
    , "from user_tables"
    , "where temporary='N' and dropped='NO'"
    , "order by table_name"
    ])
  let tblsExist = map (\tbl -> Db.SomeDatabasePredicate (Db.TableExistsPredicate tbl)) tbls

  columnChecks <-
    fmap mconcat . forM tbls $ \tbl -> do
      columns :: [(Text, ByteString, Maybe ByteString, Maybe Word, Maybe Word, Maybe Int, ByteString)] <- Odpi.querySimple conn (B8.unlines
          [ "select column_name"
          , ", data_type"
          , ", data_type_mod"
          , ", data_length"
          , ", data_precision"
          , ", data_scale"
          , ", nullable"
          , "from user_tab_columns"
          , "where table_name = '" <> TE.encodeUtf8 tbl <> "'"
          ])
      let colChecks = map (\(nm, typ, _, w, p, s, _) ->
            let oraDataType = oraDataTypeFromAtt typ nm w p s
            in Db.SomeDatabasePredicate
                 (Db.TableHasColumn tbl nm oraDataType :: Db.TableHasColumn OraColumnSchemaSyntax)
            ) columns
          notNullChecks = concatMap (\(nm, _, _, _, _, _, isNullable) ->
            if isNullable == "N"
            then
              [Db.SomeDatabasePredicate
                (Db.TableColumnHasConstraint tbl nm
                  (Db.constraintDefinitionSyntax Nothing Db.notNullConstraintSyntax Nothing)
                    :: Db.TableColumnHasConstraint OraColumnSchemaSyntax
                )]
            else []) columns
      pure (colChecks ++ notNullChecks)

  primaryKeys <-
    map (\(tbl, cols) -> Db.SomeDatabasePredicate (Db.TableHasPrimaryKey tbl (T.splitOn "," cols))) <$>
    Odpi.querySimple conn (B8.unlines
      [ "select c.table_name"
      ,   ", listagg(c.column_name, ',')"
      , "within group (order by c.position)"
      , "from user_constraints con"
      ,   "join user_cons_columns c on (c.constraint_name = con.constraint_name and c.owner = con.owner)"
      , "where con.constraint_type = 'P'"
      , "group by c.table_name"
      , "order by c.table_name"
      ])

  pure (tblsExist ++ columnChecks ++ primaryKeys)

-- | Write the migration given by the 'Db.MigrationSteps' to a file.
writeMigrationScript :: FilePath -> Db.MigrationSteps OraCommandSyntax () a -> IO ()
writeMigrationScript fp steps =
  let stepBs = migrateScript steps
  in BL.writeFile fp (BL.concat stepBs)

oraExpandDataType :: Db.DataType OraDataTypeSyntax a -> OraDataTypeSyntax
oraExpandDataType (Db.DataType ora) = ora

oraDataTypeFromAtt :: ByteString -> Text -> Maybe Word -> Maybe Word -> Maybe Int -> OraDataTypeSyntax
oraDataTypeFromAtt ty _ w p s
  | ty == "CHAR" = oraExpandDataType (Db.char w)
  | ty == "VARCHAR" = oraExpandDataType (Db.varchar w)
  | ty == "VARCHAR2" = oraExpandDataType (Db.varchar w)
  | ty == "NCHAR" = oraExpandDataType (Db.char w)
  | ty == "NVARCHAR2" = oraExpandDataType (Db.varchar w)
  | ty == "CLOB" = oraExpandDataType Db.characterLargeObject
  | ty == "NCLOB" = oraExpandDataType Db.characterLargeObject

  -- Numeric datatypes
  | ty == "NUMBER" = oraExpandDataType (Db.numeric $ fmap (\x -> (x, fmap fromIntegral s)) p) -- TODO
  | ty == "BINARY_FLOAT" = oraExpandDataType Db.double -- TODO: ???
  | ty == "BINARY_DOUBLE" = oraExpandDataType Db.double

  -- Date
  | ty == "DATE" = oraExpandDataType Db.date
  | ty == "TIMESTAMP" = oraExpandDataType Db.timestamp
  | ty == "TIMESTAMP WITH TIME ZONE" = oraExpandDataType Db.timestamptz
  | ty == "TIMESTAMP WITH LOCAL TIME ZONE" = oraExpandDataType Db.timestamptz

  -- LOB
  | ty == "BLOB" = oraExpandDataType Db.binaryLargeObject

  | otherwise = error $ "unsupported type " ++ show ty


-- instance IsSql92DataTypeSyntax dataTypeSyntax => HasDefaultSqlDataType dataTypeSyntax Decimal where
--   defaultSqlDataType _ _ = numericType Nothing
-- instance Db.IsSql92ColumnSchemaSyntax columnSchemaSyntax => HasDefaultSqlDataTypeConstraints columnSchemaSyntax Decimal

