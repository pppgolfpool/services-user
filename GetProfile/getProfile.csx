#r "..\Common\PppPool.Common.dll"
#r "..\Common\Microsoft.WindowsAzure.Storage.dll"
#r "Newtonsoft.Json"

using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using PppPool.Common;

// key(email, userId, name, all), value=query parameter
public static async Task<HttpResponseMessage> Run(HttpRequestMessage req, TraceWriter log)
{
    var jwt = await req.GetJwt("submitter");
    if (jwt == null)
        return req.CreateError(HttpStatusCode.Unauthorized);

    var connectionString = "UserStorage".GetEnvVar();
    var tableService = new TableService(connectionString);
    var blobService = new BlobService(connectionString);

    IDictionary<string, string> query = req.GetQueryNameValuePairs().ToDictionary(pair => pair.Key, pair => pair.Value);

    var key = query["key"].ToLower();
    var value = string.Empty;
    if (key != "all" && key != "userid")
        value = query["value"].ToLower();

    if(key == "userid")
    {
        var userId = "";
        if (!query.ContainsKey("userId"))
            userId = jwt.UserId;
        else userId = query["userId"].ToLower();

        var profileJson = await blobService.DownloadBlobAsync("profiles", $"{userId}.json");
        if (string.IsNullOrEmpty(profileJson))
            profileJson = "{}";
        ProfileEntity profileEntity = await tableService.GetEntityAsync<ProfileEntity>("profiles", "profile", userId);
        var jProfile = JObject.Parse(profileJson);
        jProfile["UserId"] = profileEntity.RowKey;
        jProfile["Name"] = profileEntity.Name;
        jProfile["Email"] = profileEntity.Email;
        return req.CreateOk(jProfile);
    }

    if(key == "email")
    {
        List<ProfileEntity> profiles = await tableService.GetPartitionAsync<ProfileEntity>("profiles", $"profile");
        var profile = profiles.FirstOrDefault(x => x.Email.Equals(value, StringComparison.OrdinalIgnoreCase));
        var userId = profile.RowKey;
        var profileJson = await blobService.DownloadBlobAsync("profiles", $"{userId}.json");
        if (string.IsNullOrEmpty(profileJson))
            profileJson = "{}";
        var jProfile = JObject.Parse(profileJson);
        jProfile["UserId"] = profile.RowKey;
        jProfile["Name"] = profile.Name;
        jProfile["Email"] = profile.Email;
        return req.CreateOk(jProfile);
    }

    if(key == "name")
    {
        List<ProfileEntity> profiles = await tableService.GetPartitionAsync<ProfileEntity>("profiles", $"profile");
        var profile = profiles.FirstOrDefault(x => x.Name.Equals(value, StringComparison.OrdinalIgnoreCase));
        var userId = profile.RowKey;
        var profileJson = await blobService.DownloadBlobAsync("profiles", $"{userId}.json");
        if (string.IsNullOrEmpty(profileJson))
            profileJson = "{}";
        var jProfile = JObject.Parse(profileJson);
        jProfile["UserId"] = profile.RowKey;
        jProfile["Name"] = profile.Name;
        jProfile["Email"] = profile.Email;
        return req.CreateOk(jProfile);
    }

    if(key == "all")
    {
        List<JObject> list = new List<JObject>();
        List<ProfileEntity> profiles = await tableService.GetPartitionAsync<ProfileEntity>("profiles", $"profile");
        foreach (var profile in profiles)
        {
            var userId = profile.RowKey;
            var profileJson = await blobService.DownloadBlobAsync("profiles", $"{userId}.json");
            if (string.IsNullOrEmpty(profileJson))
                profileJson = "{}";
            var jProfile = JObject.Parse(profileJson);
            jProfile["UserId"] = profile.RowKey;
            jProfile["Name"] = profile.Name;
            jProfile["Email"] = profile.Email;
            list.Add(jProfile);
        }
        return req.CreateOk(list);
    }

    return req.CreateError(HttpStatusCode.BadRequest);
}

public class ProfileEntity : TableEntity
{

    public string Email { get; set; }
    public string Name { get; set; }
}