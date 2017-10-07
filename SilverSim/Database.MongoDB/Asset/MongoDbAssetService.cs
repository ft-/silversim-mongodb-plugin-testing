// SilverSim is distributed under the terms of the
// GNU Affero General Public License v3 with
// the following clarification and special exception.

// Linking this library statically or dynamically with other modules is
// making a combined work based on this library. Thus, the terms and
// conditions of the GNU Affero General Public License cover the whole
// combination.

// As a special exception, the copyright holders of this library give you
// permission to link this library with independent modules to produce an
// executable, regardless of the license terms of these independent
// modules, and to copy and distribute the resulting executable under
// terms of your choice, provided that you also meet, for each linked
// independent module, the terms and conditions of the license of that
// module. An independent module is a module which is not derived from
// or based on this library. If you modify this library, you may extend
// this exception to your version of the library, but you are not
// obligated to do so. If you do not wish to do so, delete this
// exception statement from your version.

using MongoDB.Bson;
using MongoDB.Driver;
using Nini.Config;
using SilverSim.Main.Common;
using SilverSim.ServiceInterfaces.Asset;
using SilverSim.ServiceInterfaces.Database;
using SilverSim.Types;
using SilverSim.Types.Asset;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;

namespace SilverSim.Database.MongoDB.Asset
{
    [PluginName("Assets")]
    public sealed class MongoDbAssetService : AssetServiceInterface, IAssetMetadataServiceInterface, IAssetDataServiceInterface, IPlugin, IDBServiceInterface
    {
        private MongoClient m_Client;
        private IMongoCollection<BsonDocument> m_AssetRefs;
        private IMongoCollection<BsonDocument> m_Assets;
        private readonly string m_ConnectionString;
        private readonly string m_DatabaseName;

        public MongoDbAssetService(IConfig config)
        {
            m_ConnectionString = config.GetString("ConnectionString");
            m_DatabaseName = config.GetString("Database");
            References = new DefaultAssetReferencesService(this);
        }

        public override AssetData this[UUID key]
        {
            get
            {
                AssetData data;
                if(!TryGetValue(key, out data))
                {
                    throw new AssetNotFoundException(key);
                }
                return data;
            }
        }

        AssetMetadata IAssetMetadataServiceInterface.this[UUID key]
        {
            get
            {
                AssetMetadata metadata;
                if(!Metadata.TryGetValue(key, out metadata))
                {
                    throw new AssetNotFoundException(key);
                }
                return metadata;
            }
        }

        Stream IAssetDataServiceInterface.this[UUID key]
        {
            get
            {
                Stream s;
                if(!Data.TryGetValue(key, out s))
                {
                    throw new AssetNotFoundException(key);
                }
                return s;
            }
        }

        public override IAssetMetadataServiceInterface Metadata => this;

        public override AssetReferencesServiceInterface References { get; }

        public override IAssetDataServiceInterface Data => this;

        public override void Delete(UUID id)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("id", id.ToString());
            m_AssetRefs.DeleteOne(filter);
        }

        public override bool Exists(UUID key)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("id", key.ToString());
            var result = m_AssetRefs.Find(filter).ToList();
            return result.Count > 0;
        }

        public override Dictionary<UUID, bool> Exists(List<UUID> assets)
        {
            var result = new Dictionary<UUID, bool>();
            foreach(UUID asset in assets)
            {
                result.Add(asset, Exists(asset));
            }
            return result;
        }

        public override void Store(AssetData asset)
        {
            string hash;
            using (SHA1 sha1 = SHA1.Create())
            {
                hash = sha1.ComputeHash(asset.Data).ToHexString();
            }

            var ref_document = new BsonDocument
            {
                { "id", asset.ID.ToString() },
                { "local", asset.Local },
                { "temporary", asset.Temporary },
                { "type", (int)asset.Type },
                { "name", asset.Name },
                { "flags", (int)asset.Flags },
                { "create_time", (long)asset.CreateTime.AsULong },
                { "access_time", (long)asset.AccessTime.AsULong },
                { "hash", hash }
            };
            var asset_document = new BsonDocument
            {
                { "hash", hash },
                { "data", asset.Data }
            };

            m_Assets.ReplaceOne(
                Builders<BsonDocument>.Filter.Eq("hash", hash), 
                asset_document,
                new UpdateOptions
                {
                    IsUpsert = true
                });
            m_AssetRefs.ReplaceOne(
                Builders<BsonDocument>.Filter.Eq("id", asset.ID.ToString()),
                ref_document,
                new UpdateOptions
                {
                    IsUpsert = true
                });
        }

        public override bool TryGetValue(UUID key, out AssetData assetData)
        {
            string hash;
            assetData = new AssetData();
            if(!TryGetMetadata(key, assetData, out hash))
            {
                assetData = null;
                return false;
            }
            byte[] data;
            if(!TryGetData(hash, out data))
            {
                assetData = null;
            }
            assetData.Data = data;
            return true;
        }

        bool IAssetMetadataServiceInterface.TryGetValue(UUID key, out AssetMetadata metadata)
        {
            string hash;
            metadata = new AssetMetadata();
            if(!TryGetMetadata(key, metadata, out hash))
            {
                metadata = null;
                return false;
            }
            return true;
        }

        bool IAssetDataServiceInterface.TryGetValue(UUID key, out Stream s)
        {
            string hash;
            byte[] data;
            if(TryGetHash(key, out hash) && TryGetData(hash, out data))
            {
                s = new MemoryStream(data);
                return true;
            }
            s = null;
            return false;
        }

        private bool TryGetData(string hash, out byte[] data)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("hash", hash);
            var result = m_Assets.Find(filter).ToList();
            if (result.Count == 0)
            {
                data = null;
                return false;
            }
            BsonDocument asset = result[0];
            data = asset.GetValue("data").AsByteArray;
            return true;
        }

        private bool TryGetMetadata(UUID key, AssetMetadata metadata, out string hash)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("id", key.ToString());
            var result = m_AssetRefs.Find(filter).ToList();
            if(result.Count == 0)
            {
                hash = null;
                return false;
            }
            BsonDocument assetref = result[0];
            metadata.ID = key;
            metadata.Name = assetref.GetValue("name").AsString;
            metadata.Temporary = assetref.GetValue("temporary").AsBoolean;
            metadata.Local = assetref.GetValue("local").AsBoolean;
            metadata.Flags = (AssetFlags)assetref.GetValue("flags").AsInt32;
            metadata.AccessTime = Date.UnixTimeToDateTime((ulong)assetref.GetValue("access_time").AsInt64);
            metadata.CreateTime = Date.UnixTimeToDateTime((ulong)assetref.GetValue("create_time").AsInt64);
            metadata.Type = (AssetType)assetref.GetValue("type").AsInt32;
            hash = assetref.GetValue("hash").AsString;
            return true;
        }

        private bool TryGetHash(UUID key, out string hash)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("id", key.ToString());
            var result = m_AssetRefs.Find(filter).ToList();
            if (result.Count == 0)
            {
                hash = null;
                return false;
            }
            BsonDocument assetref = result[0];
            hash = assetref.GetValue("hash").AsString;
            return true;
        }

        public void Startup(ConfigurationLoader loader)
        {
            /* intentionally left empty */
        }

        public void VerifyConnection()
        {
            m_Client = new MongoClient(m_ConnectionString);
        }

        public void ProcessMigrations()
        {
            var database = m_Client.GetDatabase(m_DatabaseName);
            m_AssetRefs = database.GetCollection<BsonDocument>("assetrefs");
            m_Assets = database.GetCollection<BsonDocument>("assets");
        }
    }
}
