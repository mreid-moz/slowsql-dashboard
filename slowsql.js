/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
"use strict";

var slowsql_csv = null;

var displayNames = {
 thread_type: "Thread",
 submission_date: "Submission Date",
 app_name: "App",
 app_version: "Version",
 app_update_channel: "Channel",
 query: "SQL String",
 document_count: "SQL Occurence Count",
 total_invocations: "Total SQL Exec Count",
 total_duration: "Total Duration (ms)",
 median_duration: "Median Duration (ms)",
 total_documents: "Total Documents",
 occurence_rate: "% of Docs",
 database: "Database Name",
};
var tooltips = {
 thread_type: "Whether the SQL was run on the main thread or on another thread",
 submission_date: "Date when the telemetry pings were submitted",
 app_name: "Application Name (Firefox, Fennec, etc)",
 app_version: "Application Version",
 app_update_channel: "Application Update Channel",
 query: "Sanitized query string",
 document_count: "Total number of pings containing this query",
 total_invocations: "Total number of times this query was executed by all reporting pings",
 total_duration: "Total duration of all query executions",
 median_duration: "Median of per-ping durations",
 total_documents: "Total number of documents with the same Submission Date, App, Version, and Channel",
 occurence_rate: "Percentage of total pings that contained this query.",
 database: "SQLite database name (or UNKNOWN if it could not be determined)",
};

function getKey(arr) {
    return arr.slice(1,5).join(",");
}

function getDatabase(query) {
    var match = query.match(/^.*\/\* ([^ ]+) \*\/.*$/);
    if (match) {
        return match[1];
    }
    var match = query.match(/^Untracked SQL for (.+)$/);
    if (match) {
        return match[1];
    }
    return "UNKNOWN";
}

function process(raw_data) {
    var csv = $.csv.toArrays(raw_data);
    var totals = {}
    var header = csv.shift();
    header.push("total_documents");
    header.push("occurence_rate");
    header.push("database");
    //header.push("key");
    for (var i = 0; i < csv.length; i++) {
        if (csv[i][0] == "TOTAL") {
            // TOTAL,20140110,Fennec,29.0a1,nightly,ALL_PINGS,3515,3515,0,0
            totals[getKey(csv[i])] = csv[i][6];
        }
    }

    //alert(JSON.stringify(csv));
    var csv = csv.filter(function(row){
        //alert(row);
        return row[0] != "TOTAL";
    });

    // Insert doc totals:
    for (var i = 0; i < csv.length; i++) {
        var total_docs = totals[getKey(csv[i])];
        if (total_docs === null) {
            total_docs = 0;
        }
        csv[i].push(total_docs);
        csv[i].push(((csv[i][6] / total_docs) * 100).toFixed(2) + '%');
        csv[i].push(getDatabase(csv[i][5]));
        //csv[i].push(getKey(csv[i]));
    }
    return { totals: totals, data: csv, header: header };
}

function generateTable(csv) {
    var html = '<table class="tablesorter data" id="slowtable"><thead><tr>';
    for (var i = 0; i < csv.header.length; i++) {
        html += "<th title=\"" + tooltips[csv.header[i]] + "\">" + displayNames[csv.header[i]] + "</th>";
    }
    html += "</tr></thead><tbody>"
    for (var i = 0; i < csv.data.length; i++) {
        html += "<tr>\n";
        for (var j = 0; j < csv.data[i].length; j++) {
            html += "<td>" + csv.data[i][j] + "</td>";
        }
        html += "</tr>\n";
    }
    html += "</tbody></table>";
    return html;
}

function zpad(aNum) {
    return (aNum < 10 ? "0" : "") + aNum;
}

function yyyymmdd(aDate) {
    var year = aDate.getUTCFullYear();
    var month = aDate.getUTCMonth() + 1;
    var day = aDate.getUTCDate();
    return "" + year + zpad(month) + zpad(day);
}

function aggregate_days(per_day_data) {
    var keys = Object.keys(per_day_data);
    for (var i = 0; i < keys.length; i++) {
        var key = keys[i];
        if (per_day_data[key] === false) {
            console.log("data isn't finished yet.")
            return;
        }
    };
    console.log("aggregating some effin data!");
    // Filter all rows by specified filters
    // Collect rows by common query
    // Update "this week"
}

function updateData(start, end) {
    console.log("Updating date range: " + start + " to " + end );
    var s = new Date(start);
    var e = new Date(end);
    var dates = [];
    for (var d = new Date(s); d <= e; d.setDate(d.getDate() + 1)) {
        var formatted = yyyymmdd(d);
        console.log("Adding a date: " + formatted);
        dates.push(formatted);
    }
    $('#throbber').fadeIn(500);

    var per_day = {};
    dates.forEach(function(item){
        per_day[item] = false;
    });

    dates.forEach(function(item){
        console.log("fetching data for " + item);
        fetch_data(per_day, item, aggregate_days);
    });
}

function fetch_data(per_day, currentDay, callback) {
    console.log("Current day: " + currentDay);
    var xhr = new XMLHttpRequest();
    var url = "https://s3-us-west-2.amazonaws.com/telemetry-public-analysis/slowsql/data/slowsql" + currentDay + ".csv.gz";
    xhr.open("GET", url, true);
    xhr.onload = function() {
        console.log("onload:" + xhr.status);
        if (xhr.status != 200) {
            console.log("Failed to load " + url);
            per_day[currentDay] = {data: [], err: "failed to load " + url}
        } else {
            console.log("Got the data for " + url + ", processing");
            //console.log(xhr.responseText.substring(1, 50));
            per_day[currentDay] = {data: $.csv.toArrays(xhr.responseText)};
            console.log("done processing for " + currentDay + ", got " + per_day[currentDay].data.length + " rows");
            callback(per_day);
            //$('#slowsql_data').empty();
            // $('#slowsql_data').html(generateTable(slowsql_csv));
            // $('#slowtable').tablesorter({
            //     //showProcessing: true,
            //     sortList: [[11,1]],
            //     sortInitialOrder: 'desc',
            //     theme: 'blue',
            //     widgets: ['filter'],
            //     widgetOptions: {
            //        filter_searchDelay: 600
            //     },
            //     //widthFixed: true,
            // });
        }
        $('#throbber').fadeOut(500);
        $('#slowsql_data').fadeIn(500);
    };
    xhr.onerror = function(e) {
        throw new Error("failed to retrieve file:" + e);
        $('#throbber').fadeOut(500);
        $('#slowsql_data').fadeIn(500);
    };
    xhr.send(null);
}

$(function () {
    $(document).tooltip({delay: 1000});
    $("#update_date_range").click(function(){
        var start = $( "#startdate" ).val();
        var end = $('#enddate').val();
        updateData(start, end);
        console.log("Updating date range to " + start + " to " + end);
    });
    $(".datepicker").datepicker({dateFormat: "yy-mm-dd", maxDate: -1});
    $(".datepicker").datepicker("setDate", -1);
    window.setTimeout(function () {
        var start = $( "#startdate" ).val();
        var end = $('#enddate').val();
        updateData(start, end);
    }, 100);
});
