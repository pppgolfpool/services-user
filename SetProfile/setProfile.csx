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

public static async Task<HttpResponseMessage> Run(HttpRequestMessage req, TraceWriter log)
{
    var jwt = await req.GetJwt("submitter");
    if (jwt == null)
        return req.CreateError(HttpStatusCode.Unauthorized);
    var userId = jwt.UserId;

    IDictionary<string, string> query = req.GetQueryNameValuePairs().ToDictionary(pair => pair.Key, pair => pair.Value);
    var content = await req.Content.ReadAsStringAsync();
    if (string.IsNullOrEmpty(content))
        content = "{}";
    JObject jData = JObject.Parse(content);

    // Profiles can be edited by admin users. If a userId is supplied with the
    // request, use that userId, instead of the one making the pick.
    var adminJwt = await req.GetJwt("admin");
    if (adminJwt != null)
    {
        
        if (query.ContainsKey("userId"))
            userId = query["userId"];
    }
    else
    {
        jData["UserId"] = jwt.UserId;
        jData["Name"] = jwt.Name;
        jData["Email"] = jwt.Email;
    }    

    var blobService = new BlobService("UserStorage".GetEnvVar());
    var tableService = new TableService("UserStorage".GetEnvVar());

    var picksJson = await blobService.DownloadBlobAsync("profiles", $"{userId}.json");
    if (string.IsNullOrEmpty(picksJson))
        picksJson = "{}";

    ProfileEntity profileEntity = await tableService.GetEntityAsync<ProfileEntity>("profiles", "profile", userId);
    profileEntity.Email = jwt.Email;
    profileEntity.Name = jwt.Name;

    JObject jProfile = JObject.Parse(picksJson);

    foreach (var property in jData.Properties())
    {
        jProfile[property.Name] = property.Value;
    }

    if (query.ContainsKey("remove"))
    {
        var value = query["remove"].Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
        foreach (var remove in value)
        {
            if (remove == "Name" || remove == "Email" || remove == "UserId")
                continue;
            var prop = jProfile[remove];
            if (prop != null)
                jProfile.Property(remove).Remove();
        }
    }

    var profileJson = jProfile.ToString(Formatting.Indented);
    await blobService.UploadBlobAsync("profiles", $"{userId}.json", profileJson);

    await tableService.UpsertEntityAsync("profiles", profileEntity);

    return req.CreateOk(jProfile);
}

public class ProfileEntity : TableEntity
{

    public string Email { get; set; }
    public string Name { get; set; }
}