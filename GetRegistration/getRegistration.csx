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
    var jwt = await req.GetJwt("admin");
    if (jwt == null)
        return req.CreateError(HttpStatusCode.Unauthorized);

    var connectionString = "UserStorage".GetEnvVar();
    var tableService = new TableService(connectionString);
    var blobService = new BlobService(connectionString);

    var tournamentsUrl = "TournamentsUrl".GetEnvVar();
    var tournaments = await RestService.AuthorizedPostAsync($"{tournamentsUrl}/api/GetTournaments", new Dictionary<string, string>
    {
        ["season"] = "current",
        ["tour"] = "PGA TOUR",
        ["key"] = "state",
        ["value"] = "picking,progressing,completed",
    }, "ServiceToken".GetEnvVar());

    var tournament = tournaments.First();

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
        var nameSplit = profile.Name.Split(new[] { ' ' });
        jProfile["LastFirst"] = nameSplit.Last() + ", " + nameSplit.First();
        if(jProfile["isTest"] == null)
            list.Add(jProfile);
    }

    var picksUrl = "PicksUrl".GetEnvVar();
    List<JObject> picksList = new List<JObject>();
    JArray picks = (JArray)(await RestService.AuthorizedPostAsync($"{picksUrl}/api/GetPicks", new Dictionary<string, string>
    {
        ["season"] = "current",
        ["tour"] = "PGA TOUR",
        ["tournamentIndex"] = (string)tournament["Index"],
    }, "ServiceToken".GetEnvVar()));

    foreach (var item in picks)
    {
        picksList.Add((JObject)item);
    }

    foreach (var profile in list)
    {
        var pick = picksList.SingleOrDefault(x => (string)x["UserId"] == (string)profile["UserId"]);
        if (pick != null)
            profile["Picked"] = true;
    }

    return req.CreateOk(list);
}

public class ProfileEntity : TableEntity
{

    public string Email { get; set; }
    public string Name { get; set; }
    public bool Picked { get; set; }
}