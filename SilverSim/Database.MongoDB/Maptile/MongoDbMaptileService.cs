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
using SilverSim.ServiceInterfaces.Database;
using SilverSim.ServiceInterfaces.Maptile;
using SilverSim.Types;
using SilverSim.Types.Maptile;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;

namespace SilverSim.Database.MongoDB.Maptile
{
    [PluginName("Maptile")]
    [Description("MongoDB maptile backend connector")]
    public sealed class MongoDbMaptileService : MaptileServiceInterface, IPlugin, IDBServiceInterface
    {
        private MongoClient m_Client;
        private IMongoCollection<BsonDocument> m_Maptiles;
        private readonly string m_ConnectionString;
        private readonly string m_DatabaseName;

        public MongoDbMaptileService(IConfig config)
        {
            m_ConnectionString = config.GetString("ConnectionString");
            m_DatabaseName = config.GetString("Database");
        }

        public override List<MaptileInfo> GetUpdateTimes(UUID scopeid, GridVector minloc, GridVector maxloc, int zoomlevel)
        {
            var result = m_Maptiles.Find(
                Builders<BsonDocument>.Filter.Gte("locx", (int)minloc.X) &
                Builders<BsonDocument>.Filter.Lte("locx", (int)maxloc.X) &
                Builders<BsonDocument>.Filter.Gte("locy", (int)minloc.Y) &
                Builders<BsonDocument>.Filter.Lte("locy", (int)minloc.Y) &
                Builders<BsonDocument>.Filter.Eq("zoomlevel", zoomlevel) &
                Builders<BsonDocument>.Filter.Eq("scopeid", scopeid.ToString())).ToList();

            var infos = new List<MaptileInfo>();
            foreach(BsonDocument doc in result)
            {
                var info = new MaptileInfo
                {
                    Location = new GridVector((uint)doc.GetValue("locx").AsInt32, (uint)doc.GetValue("locy").AsInt32),
                    ZoomLevel = doc.GetValue("zoomlevel").AsInt32,
                    ScopeID = doc.GetValue("scopeid").AsString,
                    LastUpdate = Date.UnixTimeToDateTime((ulong)doc.GetValue("lastupdate").AsInt64)
                };
                infos.Add(info);
            }
            return infos;
        }

        public void ProcessMigrations()
        {
            var database = m_Client.GetDatabase(m_DatabaseName);
            m_Maptiles = database.GetCollection<BsonDocument>("maptiles");
        }

        public override bool Remove(UUID scopeid, GridVector location, int zoomlevel)
        {
            var deleteResult = m_Maptiles.DeleteOne(Builders<BsonDocument>.Filter.Eq("locx", (int)location.X) &
                Builders<BsonDocument>.Filter.Eq("locy", (int)location.Y) &
                Builders<BsonDocument>.Filter.Eq("zoomlevel", zoomlevel) &
                Builders<BsonDocument>.Filter.Eq("scopeid", scopeid.ToString()));
            return deleteResult.DeletedCount != 0;
        }

        public void Startup(ConfigurationLoader loader)
        {
            /* intentionally left empty */
        }

        public override void Store(MaptileData data)
        {
            var doc = new BsonDocument
            {
                { "locx", data.Location.X },
                { "locy", data.Location.Y },
                { "zoomlevel", data.ZoomLevel },
                { "scopeid",  data.ScopeID.ToString() },
                { "contenttype", data.ContentType },
                { "data", data.Data },
                { "lastupdate", (long)Date.Now.AsULong }
            };
            m_Maptiles.UpdateOne(
                Builders<BsonDocument>.Filter.Eq("locx", (int)data.Location.X) &
                Builders<BsonDocument>.Filter.Eq("locy", (int)data.Location.Y) &
                Builders<BsonDocument>.Filter.Eq("zoomlevel", data.ZoomLevel) &
                Builders<BsonDocument>.Filter.Eq("scopeid", data.ScopeID.ToString()),
                doc,
                new UpdateOptions { IsUpsert = true });
        }

        public override bool TryGetValue(UUID scopeid, GridVector location, int zoomlevel, out MaptileData data)
        {
            var result = m_Maptiles.Find(Builders<BsonDocument>.Filter.Eq("locx", (int)location.X) &
                Builders<BsonDocument>.Filter.Eq("locy", (int)location.Y) &
                Builders<BsonDocument>.Filter.Eq("zoomlevel", zoomlevel) &
                Builders<BsonDocument>.Filter.Eq("scopeid", scopeid.ToString())).ToList();
            if(result.Count == 0)
            {
                data = null;
                return false;
            }

            BsonDocument doc = result[0];
            data = new MaptileData
            {
                Location = new GridVector((uint)doc.GetValue("locx").AsInt32, (uint)doc.GetValue("locy").AsInt32),
                ZoomLevel = doc.GetValue("zoomlevel").AsInt32,
                ScopeID = doc.GetValue("scopeid").AsString,
                ContentType = doc.GetValue("contenttype").AsString,
                Data = doc.GetValue("data").AsByteArray,
                LastUpdate = Date.UnixTimeToDateTime((ulong)doc.GetValue("lastupdate").AsInt64)
            };
            return true;
        }

        public void VerifyConnection()
        {
            try
            {
                m_Client = new MongoClient(m_ConnectionString);
            }
            catch (FileNotFoundException)
            {
                if (VersionInfo.IsPlatformMono)
                {
                    throw new ConfigurationLoader.ConfigurationErrorException("MongoDB plugin needs at least Mono 4.4");
                }
                throw;
            }
        }
    }
}
