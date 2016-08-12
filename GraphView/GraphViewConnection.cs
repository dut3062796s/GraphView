// GraphView
// 
// Copyright (c) 2015 Microsoft Corporation
// 
// All rights reserved. 
// 
// MIT License
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
// 

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using MySql.Data;
using MySql.Data.MySqlClient;
using JsonServer;

namespace GraphView
{
    public abstract class GraphViewConnection
    {
        internal DBPortal portal;
        public abstract void Open();
        public abstract void Close();
        public abstract void CreateCollection(string CollectionName);
        public abstract void DeleteCollection(string CollectionName);
    }

    public class GraphViewDocDbConnection : GraphViewConnection
    {
        internal DocumentClient DocDbClient;
        internal string EndPointUrl;
        internal string AuthorizationKey;
        internal string DatabaseID;
        public GraphViewDocDbConnection(string pEndPointUrl, string pAuthorizationKey, string pDatabaseID)
        {
            portal = new DocDbPortal(this);
            EndPointUrl = pEndPointUrl;
            AuthorizationKey = pAuthorizationKey;
            DatabaseID = pDatabaseID;
        }
        public override void Open()
        {
            DocDbClient = new DocumentClient(new Uri(EndPointUrl), AuthorizationKey);
        }

        public override void Close()
        {
            DocDbClient = null;
        }

        public override async void CreateCollection(string CollectionName)
        {
            DocumentCollection DocDBCollection =
                DocDbClient.CreateDocumentCollectionQuery("dbs/" + DatabaseID)
                    .Where(c => c.Id == CollectionName)
                    .AsEnumerable()
                    .FirstOrDefault();

            DocDBCollection = DocDBCollection ?? 
                await DocDbClient.CreateDocumentCollectionAsync(
                    "dbs/" + DatabaseID,
                    new DocumentCollection
                    {
                        Id = CollectionName
                    });
        }

        public override async void DeleteCollection(string CollectionName)
        {
            DocumentCollection DocDBCollection =
                DocDbClient.CreateDocumentCollectionQuery("dbs/" + DatabaseID)
                    .Where(c => c.Id == CollectionName)
                    .AsEnumerable()
                    .FirstOrDefault();

            if(DocDBCollection != null)
                await DocDbClient.DeleteDocumentCollectionAsync(DocDBCollection.SelfLink);
        }
    }

    public class GraphViewMariaDBConnection : GraphViewConnection
    {
        internal JsonServerConnection MariaDBConnection;
        private string CollectionID;
        public GraphViewMariaDBConnection(string ConnectionString)
        {
            portal = new MariaDbPortal(this);
            MariaDBConnection = new JsonServerConnection(ConnectionString, DatabaseType.Mariadb);
        }
        public override void Open()
        {
            MariaDBConnection.Open();
        }

        public override void Close()
        {
            MariaDBConnection.Close();
        }

        public override void CreateCollection(string CollectionName)
        {
            if (!MariaDBConnection.ContainsCollection(CollectionName))
            {
                MariaDBConnection.CreateCollection(CollectionName);
            }
        }

        public override void DeleteCollection(string CollectionName)
        {
            MariaDBConnection.Open();

            if (MariaDBConnection.ContainsCollection(CollectionName))
            {
                MariaDBConnection.DeleteCollection(CollectionName);
            }
        }
    }
}