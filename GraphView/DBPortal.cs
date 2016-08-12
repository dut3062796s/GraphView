using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JsonServer.JsonQuery;
using Microsoft.Azure.Documents.Client;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Newtonsoft.Json.Linq;

namespace GraphView
{
    public abstract class DBPortal
    {

        public abstract void InsertDocument(string doc, string collection);
        public abstract void DeleteDocument(string DocID, string collection);
        public abstract void ReplaceDocument(string DocID, string NewDoc, string collection);
        public abstract IDataReader RetriveDocument(string collection, string script);
        public abstract string ConsturctDeleteNodeScript();
        public abstract void TranslateScriptSegment(List<string> ProcessedNodeList, MatchNode node, List<string> header,
            int pStartOfResultField);

    }

    public class DocDbPortal : DBPortal
    {
        private GraphViewDocDbConnection connection;

        public DocDbPortal(GraphViewDocDbConnection pConnection)
        {
            connection = pConnection;
        }
        public override async void InsertDocument(string doc, string collection)
        {
            await connection.DocDbClient.CreateDocumentAsync("dbs/" + connection.DatabaseID + "/colls/" + collection, doc);
        }

        public override async void DeleteDocument(string DocID, string collection)
        {
            await connection.DocDbClient.DeleteDocumentAsync("dbs/" + connection.DatabaseID + "/colls/" + collection + "/docs/" + DocID); 
        }

        public override void ReplaceDocument(string DocID, string NewDoc, string collection)
        {
            DeleteDocument(DocID,collection);
            InsertDocument(NewDoc,collection);
        }

        public override IDataReader RetriveDocument(string collection, string script)
        {
            FeedOptions QueryOptions = new FeedOptions { MaxItemCount = -1 };
            IQueryable<dynamic> Result = connection.DocDbClient.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(connection.DatabaseID, collection), script, QueryOptions);
            return new DocDbReader(Result.ToList());
        }

        public override void TranslateScriptSegment(List<string> ProcessedNodeList, MatchNode node, List<string> header,
            int pStartOfResultField)
        {
            // Node predicates will be attached here.
            string FromClauseString = node.NodeAlias;
            string WhereClauseString = "";
            //string AttachedClause = "From " + node.NodeAlias;
            string PredicatesOnReverseEdge = "";
            string PredicatesOnNodes = "";
            foreach (var edge in node.ReverseNeighbors.Concat(node.Neighbors))
            {
                if (node.ReverseNeighbors.Contains(edge))
                    FromClauseString += " Join " + edge.EdgeAlias + " in " + node.NodeAlias + "._reverse_edge ";
                else
                    FromClauseString += " Join " + edge.EdgeAlias + " in " + node.NodeAlias + "._edge ";
                if (edge != node.ReverseNeighbors.Concat(node.Neighbors).Last())
                    foreach (var predicate in edge.Predicates)
                    {
                        PredicatesOnReverseEdge += predicate + " AND ";
                    }
                else
                    foreach (var predicate in edge.Predicates)
                    {
                        if (predicate != edge.Predicates.Last())
                            PredicatesOnReverseEdge += predicate + " AND ";
                        else PredicatesOnReverseEdge += predicate;
                    }
            }

            FromClauseString = " FROM " + FromClauseString;

            foreach (var predicate in node.Predicates)
            {
                if (predicate != node.Predicates.Last())
                    PredicatesOnNodes += predicate + " AND ";
                else
                    PredicatesOnNodes += predicate;
            }
            if (PredicatesOnNodes != "" || PredicatesOnReverseEdge != "")
            {
                WhereClauseString += " WHERE ";
                if (PredicatesOnNodes != "" && PredicatesOnReverseEdge != "")
                    WhereClauseString += PredicatesOnNodes + " AND " + PredicatesOnReverseEdge;
                else WhereClauseString += PredicatesOnNodes + PredicatesOnReverseEdge;
            }

            // Select elements that related to current node will be attached here.

            List<string> ResultIndexToAppend = new List<string>();
            foreach (string ResultIndex in header.GetRange(pStartOfResultField, header.Count - pStartOfResultField))
            {
                int CutPoint = ResultIndex.Length;
                if (ResultIndex.IndexOf('.') != -1) CutPoint = ResultIndex.IndexOf('.');
                if (ResultIndex.Substring(0, CutPoint) == node.NodeAlias)
                    ResultIndexToAppend.Add(ResultIndex);
                foreach (var edge in node.ReverseNeighbors)
                {
                    if (ResultIndex.Substring(0, CutPoint) == edge.EdgeAlias)
                        ResultIndexToAppend.Add(ResultIndex);
                }
            }

            string ResultIndexString = ",";
            foreach (string ResultIndex in ResultIndexToAppend)
            {
                if (ResultIndex.Substring(ResultIndex.Length - 3, 3) == "doc")
                    ResultIndexString += ResultIndex.Substring(0, ResultIndex.Length - 4) + " AS " + ResultIndex.Replace(".", "_") + ",";
                else ResultIndexString += ResultIndex + " AS " + ResultIndex.Replace(".", "_") + ",";
            }
            if (ResultIndexString == ",") ResultIndexString = "";
            ResultIndexString = CutTheTail(ResultIndexString);

            // Reverse checking related script will be attached here.
            string ReverseCheckString = ",";
            foreach (var ReverseEdge in node.ReverseNeighbors.Concat(node.Neighbors))
            {
                if (ProcessedNodeList.Contains(ReverseEdge.SinkNode.NodeAlias))
                    ReverseCheckString += ReverseEdge.EdgeAlias + " AS " + ReverseEdge.EdgeAlias + "_REV,";
            }
            if (ReverseCheckString == ",") ReverseCheckString = "";
            ReverseCheckString = CutTheTail(ReverseCheckString);

            // The DocDb script that related to the giving node will be assembled here.
            string ScriptBase = "SELECT {\"id\":node.id, \"edge\":node._edge, \"reverse\":node._reverse_edge} AS NodeInfo";
            string QuerySegment = QuerySegment = ScriptBase.Replace("node", node.NodeAlias) + ResultIndexString + " " + ReverseCheckString;
            QuerySegment += FromClauseString + WhereClauseString;
            node.AttachedQuerySegment = QuerySegment;

        }
        private string CutTheTail(string InRangeScript)
        {
            if (InRangeScript.Length == 0) return "";
            return InRangeScript.Substring(0, InRangeScript.Length - 1);
        }
    }

    public class MariaDbPortal : DBPortal
    {
        private GraphViewMariaDBConnection connection;

        public MariaDbPortal(GraphViewMariaDBConnection pConnection)
        {
            connection = pConnection;
        }
        public override async void InsertDocument(string doc, string collection)
        {
            connection.MariaDBConnection.InsertJson(doc,collection);
        }

        public override async void DeleteDocument(string DocID, string collection)
        {
            throw new NotImplementedException();
        }

        public override void ReplaceDocument(string DocID, string NewDoc, string collection)
        {
            DeleteDocument(DocID, collection);
            InsertDocument(NewDoc, collection);
        }

        public override IDataReader RetriveDocument(string collection, string script)
        {
            return connection.MariaDBConnection.ExecuteReader(script);
        }
        public override void TranslateScriptSegment(List<string> ProcessedNodeList, MatchNode node, List<string> header,
    int pStartOfResultField)
        {
            // Node predicates will be attached here.
            string FromClauseString = node.NodeAlias;
            string WhereClauseString = "";
            //string AttachedClause = "From " + node.NodeAlias;
            string PredicatesOnReverseEdge = "";
            string PredicatesOnNodes = "";
            foreach (var edge in node.ReverseNeighbors.Concat(node.Neighbors))
            {
                if (node.ReverseNeighbors.Contains(edge))
                    FromClauseString += " Join " + edge.EdgeAlias + " in " + node.NodeAlias + "._reverse_edge ";
                else
                    FromClauseString += " Join " + edge.EdgeAlias + " in " + node.NodeAlias + "._edge ";
                if (edge != node.ReverseNeighbors.Concat(node.Neighbors).Last())
                    foreach (var predicate in edge.Predicates)
                    {
                        PredicatesOnReverseEdge += predicate + " AND ";
                    }
                else
                    foreach (var predicate in edge.Predicates)
                    {
                        if (predicate != edge.Predicates.Last())
                            PredicatesOnReverseEdge += predicate + " AND ";
                        else PredicatesOnReverseEdge += predicate;
                    }
            }

            FromClauseString = " FROM " + FromClauseString;

            foreach (var predicate in node.Predicates)
            {
                if (predicate != node.Predicates.Last())
                    PredicatesOnNodes += predicate + " AND ";
                else
                    PredicatesOnNodes += predicate;
            }
            if (PredicatesOnNodes != "" || PredicatesOnReverseEdge != "")
            {
                WhereClauseString += " WHERE ";
                if (PredicatesOnNodes != "" && PredicatesOnReverseEdge != "")
                    WhereClauseString += PredicatesOnNodes + " AND " + PredicatesOnReverseEdge;
                else WhereClauseString += PredicatesOnNodes + PredicatesOnReverseEdge;
            }

            // Select elements that related to current node will be attached here.

            List<string> ResultIndexToAppend = new List<string>();
            foreach (string ResultIndex in header.GetRange(pStartOfResultField, header.Count - pStartOfResultField))
            {
                int CutPoint = ResultIndex.Length;
                if (ResultIndex.IndexOf('.') != -1) CutPoint = ResultIndex.IndexOf('.');
                if (ResultIndex.Substring(0, CutPoint) == node.NodeAlias)
                    ResultIndexToAppend.Add(ResultIndex);
                foreach (var edge in node.ReverseNeighbors)
                {
                    if (ResultIndex.Substring(0, CutPoint) == edge.EdgeAlias)
                        ResultIndexToAppend.Add(ResultIndex);
                }
            }

            string ResultIndexString = ",";
            foreach (string ResultIndex in ResultIndexToAppend)
            {
                if (ResultIndex.Substring(ResultIndex.Length - 3, 3) == "doc")
                    ResultIndexString += ResultIndex.Substring(0, ResultIndex.Length - 4) + " AS " + ResultIndex.Replace(".", "_") + ",";
                else ResultIndexString += ResultIndex + " AS " + ResultIndex.Replace(".", "_") + ",";
            }
            if (ResultIndexString == ",") ResultIndexString = "";
            ResultIndexString = CutTheTail(ResultIndexString);

            // Reverse checking related script will be attached here.
            string ReverseCheckString = ",";
            foreach (var ReverseEdge in node.ReverseNeighbors.Concat(node.Neighbors))
            {
                if (ProcessedNodeList.Contains(ReverseEdge.SinkNode.NodeAlias))
                    ReverseCheckString += ReverseEdge.EdgeAlias + " AS " + ReverseEdge.EdgeAlias + "_REV,";
            }
            if (ReverseCheckString == ",") ReverseCheckString = "";
            ReverseCheckString = CutTheTail(ReverseCheckString);

            // The DocDb script that related to the giving node will be assembled here.
            string ScriptBase = "SELECT {\"id\":node.id, \"edge\":node._edge, \"reverse\":node._reverse_edge} AS NodeInfo";
            string QuerySegment = QuerySegment = ScriptBase.Replace("node", node.NodeAlias) + ResultIndexString + " " + ReverseCheckString;
            QuerySegment += FromClauseString + WhereClauseString;
            node.AttachedQuerySegment = QuerySegment;

        }
        private string CutTheTail(string InRangeScript)
        {
            if (InRangeScript.Length == 0) return "";
            return InRangeScript.Substring(0, InRangeScript.Length - 1);
        }
    }
}
