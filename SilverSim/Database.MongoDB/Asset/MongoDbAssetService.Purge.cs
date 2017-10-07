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
using SilverSim.ServiceInterfaces.Purge;
using SilverSim.ServiceInterfaces.Statistics;
using SilverSim.Threading;
using SilverSim.Types;
using SilverSim.Types.Asset;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace SilverSim.Database.MongoDB.Asset
{
    public sealed partial class MongoDbAssetService : IAssetPurgeServiceInterface, IQueueStatsAccess
    {
        public void MarkAssetAsUsed(List<UUID> assetIDs)
        {
            if(assetIDs.Count == 0)
            {
                return;
            }
            var filter = Builders<BsonDocument>.Filter.Eq("id", assetIDs[0].ToString());
            for(int i = 1; i < assetIDs.Count; ++i)
            {
                filter |= Builders<BsonDocument>.Filter.Eq("id", assetIDs[i].ToString());
            }

            m_AssetRefs.UpdateMany(filter,
                new BsonDocument { { "access_time", (long)Date.Now.AsULong } });
        }

        public long PurgeUnusedAssets()
        {
            long purged = 0;
#if CHANGE_FOR_MONGO
            using (var conn = new MySqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new MySqlCommand("DELETE FROM assetrefs WHERE usesprocessed = 1 AND access_time < @access_time AND NOT EXISTS (SELECT NULL FROM assetsinuse WHERE usesid = assetrefs.id)", conn))
                {
                    ulong now = Date.GetUnixTime() - 2 * 24 * 3600;
                    cmd.Parameters.AddParameter("@access_time", now);
                    purged = cmd.ExecuteNonQuery();
                }
                using (var cmd = new MySqlCommand("DELETE FROM assetsinuse WHERE NOT EXISTS (SELECT NULL FROM assetrefs WHERE assetsinuse.id = assetrefs.id)", conn))
                {
                    cmd.ExecuteNonQuery();
                }
                using (var cmd = new MySqlCommand("DELETE FROM assetdata WHERE NOT EXISTS (SELECT NULL FROM assetrefs WHERE assetdata.hash = assetrefs.hash AND assetdata.assetType = assetrefs.assetType)", conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
#endif
            return purged;
        }

        private void GenerateAssetInUseEntries(AssetData data)
        {
            List<UUID> references = data.References;
            var refs = new List<string>();
            foreach(UUID reference in references)
            {
                refs.Add(reference.ToString());
            }

            var doc = new BsonDocument
            {
                { "references", refs.ToBson() },
                { "usesprocessed", true }
            };

            m_AssetRefs.UpdateOne(Builders<BsonDocument>.Filter.Eq("id", data.ID.ToString()),
                doc);
        }

        public List<UUID> GetUnprocessedAssets()
        {
            var filter = Builders<BsonDocument>.Filter.Eq("processed", false);
            var result = m_AssetRefs.Find(filter).ToList();
            var assets = new List<UUID>();
            foreach (BsonDocument doc in result)
            {
                assets.Add(doc["id"].AsString);
            }
            return assets;
        }

        private BlockingQueue<UUID> m_AssetProcessQueue = new BlockingQueue<UUID>();
        private int m_ActiveAssetProcessors;
        private int m_Processed;

        public void EnqueueAsset(UUID assetid)
        {
            m_AssetProcessQueue.Enqueue(assetid);
            if (m_ActiveAssetProcessors == 0)
            {
                ThreadPool.QueueUserWorkItem(AssetProcessor);
            }
        }

        private void AssetProcessor(object state)
        {
            Interlocked.Increment(ref m_ActiveAssetProcessors);
            while (m_AssetProcessQueue.Count > 0)
            {
                UUID assetid;
                try
                {
                    assetid = m_AssetProcessQueue.Dequeue(1000);
                }
                catch
                {
                    Interlocked.Decrement(ref m_ActiveAssetProcessors);
                    if (m_AssetProcessQueue.Count == 0)
                    {
                        break;
                    }
                    Interlocked.Increment(ref m_ActiveAssetProcessors);
                    continue;
                }

                AssetData asset;
                try
                {
                    asset = this[assetid];
                }
                catch
                {
                    continue;
                }

                try
                {
                    GenerateAssetInUseEntries(asset);
                }
                catch
                {
                    m_AssetProcessQueue.Enqueue(asset.ID);
                }
                Interlocked.Increment(ref m_Processed);
                asset = null; /* ensure cleanup */
            }
        }

        private QueueStat GetProcessorQueueStats()
        {
            int c = m_AssetProcessQueue.Count;
            return new QueueStat(c != 0 ? "PROCESSING" : "IDLE", c, (uint)m_Processed);
        }

        IList<QueueStatAccessor> IQueueStatsAccess.QueueStats
        {
            get
            {
                var stats = new List<QueueStatAccessor>();
                stats.Add(new QueueStatAccessor("AssetReferences", GetProcessorQueueStats));
                return stats;
            }
        }
    }
}
