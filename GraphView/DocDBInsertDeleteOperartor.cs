using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphView
{

    internal class InsertEdgeOperator : GraphViewOperator
    {
        public GraphViewOperator SelectInput;
        public string edge;
        public string source, sink;
        public GraphViewConnection dbConnection;
        internal Dictionary<string, string> map;
        public string collection;


        public InsertEdgeOperator(GraphViewConnection dbConnection, string collection, GraphViewOperator SelectInput, string edge, string source, string sink)
        {
            this.dbConnection = dbConnection;
            this.SelectInput = SelectInput;
            this.edge = edge;
            this.source = source;
            this.sink = sink;
            this.collection = collection;
            Open();
        }

        #region Unfinish GroupBy-method
        /*public override Record Next()
        {
            Dictionary<string, List<string>> groupBySource = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.List<string>>();
            Dictionary<string, List<string>> groupBySink = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.List<string>>();

            SelectInput.Open();
            while (SelectInput.Status())
            {
                Record rec = (Record)SelectInput.Next();
                string source = rec.RetriveData(null, 0);
                string sink = rec.RetriveData(null, 1);

                if (!groupBySource.ContainsKey(source))
                {
                    groupBySource[source] = new System.Collections.Generic.List<string>();
                }
                groupBySource[source].Add(sink);

                if (!groupBySink.ContainsKey(sink))
                {
                    groupBySink[sink] = new System.Collections.Generic.List<string>();
                }
                groupBySink[sink].Add(source);
            }
            SelectInput.Close();

            foreach (string source in groupBySource.Keys)
            {
                // Insert edges into the source doc
            }

            foreach (string sink in groupBySink.Keys)
            {
                // Insert reverse edges into the sink doc
            }

            return null;
        }*/
        #endregion

        public override Record Next()
        {
            if (!Status()) return null;
            map = new Dictionary<string, string>();

            while (SelectInput.Status())
            {
                //get source and sink's id from SelectQueryBlock's TraversalProcessor 
                Record rec = SelectInput.Next();
                if (rec == null) break;
                List<string> header = SelectInput.header;
                string sourceid = rec.RetriveData(header, source);
                string sinkid = rec.RetriveData(header, sink);
                string source_tital = source + ".doc";
                string sink_tital = sink + ".doc";

                string source_json_str = rec.RetriveData(header, source_tital);
                string sink_json_str = rec.RetriveData(header, sink_tital);
                
                InsertEdgeInMap(sourceid, sinkid,source_json_str, sink_json_str);
            }

            Upload();

            Close();
            return null;
        }

        internal void InsertEdgeInMap(string sourceid, string sinkid, string source_doc, string sink_doc)
        {
            if (!map.ContainsKey(sourceid))
                map[sourceid] = source_doc;
            
            if (!map.ContainsKey(sinkid))
                map[sinkid] = sink_doc;
            
            GraphViewDocDBCommand.INSERT_EDGE(map, edge, sourceid, sinkid);
        }

        internal void Upload()
        {
            dbConnection.portal.UploadFinish = false;
            ReplaceDocument();

            //Wait until finish replacing.
            while (!dbConnection.portal.UploadFinish)
                System.Threading.Thread.Sleep(10);
        }

        public async Task ReplaceDocument()
        {
            foreach (var cnt in map)
                dbConnection.portal.ReplaceDocument(cnt.Key,cnt.Value,collection);
            dbConnection.portal.UploadFinish = true;
        }
    }

    internal class DeleteEdgeOperator : GraphViewOperator
    {
        public GraphViewOperator SelectInput;
        public string source, sink;
        public GraphViewConnection dbConnection;
        private bool UploadFinish;
        public string EdgeID_str;
        public string EdgeReverseID_str;
        internal Dictionary<string, string> map;
        public string collection;

        public DeleteEdgeOperator(GraphViewConnection dbConnection, string collection, GraphViewOperator SelectInput,  string source, string sink, string EdgeID_str, string EdgeReverseID_str)
        {
            this.dbConnection = dbConnection;
            this.SelectInput = SelectInput;
            this.source = source;
            this.sink = sink;
            this.EdgeID_str = EdgeID_str;
            this.EdgeReverseID_str = EdgeReverseID_str;
            this.collection = collection;
            Open();
        }

        public override Record Next()
        {
            if (!Status()) return null;
            map = new Dictionary<string, string>();

            while (SelectInput.Status())
            {
                //get source and sink's id from SelectQueryBlock's TraversalProcessor 
                Record rec = SelectInput.Next();
                if (rec == null) break;
                List<string> header = SelectInput.header;
                string sourceid = rec.RetriveData(header, source);
                string sinkid = rec.RetriveData(header, sink);
                //The "e" in the Record is "Reverse_e" in fact
                string EdgeReverseID = rec.RetriveData(header, EdgeID_str);
                string EdgeID = rec.RetriveData(header, EdgeReverseID_str);

                //get source.doc and sink.doc
                string source_tital = source + ".doc";
                string sink_tital = sink + ".doc";
                string source_json_str = rec.RetriveData(header, source_tital);
                string sink_json_str = rec.RetriveData(header, sink_tital);

                int ID, reverse_ID;
                int.TryParse(EdgeID, out ID);
                int.TryParse(EdgeReverseID, out reverse_ID);



                DeleteEdgeInMap(sourceid,sinkid,ID,reverse_ID,source_json_str,sink_json_str);
            }

            Upload();

            Close();

            return null;
        }

        internal void DeleteEdgeInMap(string sourceid, string sinkid, int ID, int reverse_ID,string source_json_str, string sink_json_str)
        {

            //Create one if a document not exist locally.
            if (!map.ContainsKey(sourceid))
                map[sourceid] = source_json_str;
            if (!map.ContainsKey(sinkid))
                map[sinkid] = sink_json_str;

            map[sourceid] = GraphViewJsonCommand.Delete_edge(map[sourceid], ID);
            map[sinkid] = GraphViewJsonCommand.Delete_reverse_edge(map[sinkid], reverse_ID);
        }

        internal void Upload()
        {
            UploadFinish = false;
            ReplaceDocument();
            //wait until finish replacing.
            while (!UploadFinish)
                System.Threading.Thread.Sleep(10);
        }

        public async Task ReplaceDocument()
        {
            foreach (var cnt in map)
                dbConnection.portal.ReplaceDocument(cnt.Key, cnt.Value, collection);
            dbConnection.portal.UploadFinish = true;
        }
    }

    internal class InsertNodeOperator : GraphViewOperator
    {
        public string Json_str;
        public GraphViewConnection dbConnection;
        public string collection;

        public InsertNodeOperator(GraphViewConnection dbConnection, string collection, string Json_str)
        {
            this.dbConnection = dbConnection;
            this.Json_str = Json_str;
            this.collection = collection;
            Open();
        }
        public override Record Next()
        {
            if (!Status()) return null;
            
            Upload(Json_str);

            Close();
            return null;
        }

        void Upload(string Json_str)
        {
            dbConnection.portal.UploadFinish = false;
            CreateDocument(Json_str);

            //Wait until finish Creating documents.
            while (!dbConnection.portal.UploadFinish)
                System.Threading.Thread.Sleep(10);
        }

        public async Task CreateDocument(string Json_str)
        {
            dbConnection.portal.InsertDocument(Json_str, collection);
        }
    }
    internal class DeleteNodeOperator : GraphViewOperator
    {
        public WBooleanExpression search;
        public string Selectstr;
        public GraphViewConnection dbConnection;
        public string collection;

        public DeleteNodeOperator(GraphViewConnection dbConnection,string collection,WBooleanExpression search, string Selectstr)
        {
            this.dbConnection = dbConnection;
            this.search = search;
            this.Selectstr = Selectstr;
            this.collection = collection;
            Open();
        }
        private bool CheckObject(JObject Item)
        {
            JToken edge = Item["edge"];
            JToken reverse = Item["reverse"];
            foreach (var x in reverse)
                return false;
            foreach (var x in edge)
                return false;
            return true;
        }
        /// <summary>
        /// Check if there are some edges still connect to these nodes.
        /// </summary>
        /// <returns></returns>
        internal bool CheckNodes()
        {
            var sum_DeleteNode = dbConnection.portal.RetriveDocument(collection, Selectstr);

            while (sum_DeleteNode.Read())
            {
                var item = sum_DeleteNode["default"];
                if (!CheckObject((JObject) item))
                    return false;
            }
            return true;
        }

        /// <summary>
        /// Get those nodes.
        /// And then delete it.
        /// </summary>
        internal void DeleteNodes()
        {
            var sum_DeleteNode = dbConnection.portal.RetriveDocument(collection, Selectstr);

            while (sum_DeleteNode.Read())
            {
                var item = sum_DeleteNode["default"];
                JToken id = ((JObject) item)["id"];
                dbConnection.portal.UploadFinish = false;
                DeleteDocument(id.ToString());
                //wait until finish deleting
                while (!dbConnection.portal.UploadFinish)
                    System.Threading.Thread.Sleep(10);

            }
        }

        /// <summary>
        /// First check if there are some edges still connect to these nodes.
        /// If not, delete them.
        /// </summary>
        /// <returns></returns>
        public override Record Next()
        {
            if (!Status())
                return null;
            
            if (CheckNodes())
            {
                DeleteNodes();
            }
            else
            {
                Close();
                throw new GraphViewException("There are some edges still connect to these nodes.");
            }
            Close();
            return null;
        }

        public async Task DeleteDocument(string docid)
        {
            dbConnection.portal.DeleteDocument(docid,collection);
        }
    }

}
