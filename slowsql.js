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

function updateData(start, end) {
    console.log("Updating date range: " + start + " to " + end );
    $('#throbber').fadeIn(500);

    // TODO: iterate from start to end, adding in each day's data.
    var xhr = new XMLHttpRequest();
    var url = "https://s3-us-west-2.amazonaws.com/telemetry-public-analysis/slowsql/data/slowsql" + start + ".csv.gz";
    xhr.open("GET", url, true);
    xhr.onload = function() {
        console.log("onload:" + xhr.status);
        if (xhr.status != 200) {
            console.log("Failed to load " + url);
        } else {
            console.log("Got the data, processing");
            //console.log(xhr.responseText.substring(1, 50));
            slowsql_csv = process(xhr.responseText);
            console.log("done processing, creating tables");
            $('#slowsql_data').empty();
            $('#slowsql_data').html(generateTable(slowsql_csv));
            $('#slowtable').tablesorter({
                //showProcessing: true,
                sortList: [[11,1]],
                sortInitialOrder: 'desc',
                theme: 'blue',
                widgets: ['filter'],
                widgetOptions: {
                   filter_searchDelay: 600
                },
                //widthFixed: true,
            });
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
        console.log("Updating date range to " + $('#startdate').val() + " to " + $('#enddate').val() );
    });
    $(".datepicker").datepicker({dateFormat: "yymmdd", maxDate: -1});
    $(".datepicker").datepicker("setDate", -1);
    window.setTimeout(function () {
        var start = $( "#startdate" ).val();
        var end = $('#enddate').datepicker("getDate");
        updateData(start, end);
    }, 100);
});
