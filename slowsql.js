/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
"use strict";

var yesterday = new Date();
yesterday.setDate(yesterday.getDate() - 1);
//console.log("Set yesterday to " + yyyymmdd(yesterday));

// Monday is 1, so we adjust offset accordingly
var dowOffset = (yesterday.getUTCDay() - 1) % 7;
var thisWeekStart = new Date(yesterday);
var thisWeekEnd = new Date(yesterday);
thisWeekStart.setDate(thisWeekStart.getDate() - dowOffset);
thisWeekEnd.setDate(thisWeekStart.getDate() + (7 - dowOffset));
//console.log("Set this week to " + yyyymmdd(thisWeekStart) + " to " + yyyymmdd(thisWeekEnd));

var lastWeekStart = new Date(thisWeekStart);
lastWeekStart.setDate(lastWeekStart.getDate() - 7);
var lastWeekEnd = new Date(thisWeekEnd);
lastWeekEnd.setDate(lastWeekEnd.getDate() - 7);
//console.log("Set last week to " + yyyymmdd(lastWeekStart) + " to " + yyyymmdd(lastWeekEnd));

var slowsql_data = {};

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

function zpad(aNum) {
    return (aNum < 10 ? "0" : "") + aNum;
}

function yyyymmdd(aDate) {
    var year = aDate.getUTCFullYear();
    var month = aDate.getUTCMonth() + 1;
    var day = aDate.getUTCDate();
    return "" + year + zpad(month) + zpad(day);
}

function fetch_data(key, cb) {
    if (slowsql_data[key]) {
        cb();
        return;
    }
    console.log("Fetching: " + key);
    var xhr = new XMLHttpRequest();
    //var url = "https://s3-us-west-2.amazonaws.com/telemetry-public-analysis/slowsql/data/weekly" + key + ".csv.gz";
    var url = "weekly_" + key + ".csv";
    console.log("Fetching url: " + url);
    xhr.open("GET", url, true);
    xhr.onload = function() {
        console.log("onload:" + xhr.status);
        if (xhr.status != 200 && xhr.status != 0) {
            console.log("Failed to load " + url);
            slowsql_data[key] = []
        } else {
            console.log("Got the data for " + url + ", processing");
            //console.log(xhr.responseText.substring(1, 50));
            slowsql_data[key] = $.csv.toArrays(xhr.responseText);
            console.log("done processing for " + key + ", got " + slowsql_data[key].length + " rows");
        }
        $('#throbber').fadeOut(500);
        $('#slowsql_data').fadeIn(500);
        cb(key);
    };
    xhr.onerror = function(e) {
        //throw new Error("failed to retrieve file:" + e);
        console.log("Failed to fetch: " + url);
        $('#throbber').fadeOut(500);
        $('#slowsql_data').fadeIn(500);
        slowsql_data[key] = []
        cb(key);
    };
    try {
        xhr.send(null);
    } catch(e) {
        console.log("Failed to fetch: " + url);
        $('#throbber').fadeOut(500);
        $('#slowsql_data').fadeIn(500);
        slowsql_data[key] = []
        cb(key);
    }
}

function populate_this_week(key) {
    console.log("Populating this week's data table");
    var tbody = $('#current_data_table > tbody');
    tbody.empty();
    if (slowsql_data[key]) {
        for (var i = 0; i < slowsql_data[key].length; i++) {
            var rank = i + 1;
            var trow = $('<tr>', {id: "tw" + rank});
            trow.append($('<td>', {id: "tw" + rank + "rank", text: rank}));
            var drow = slowsql_data[key][i];
            for (var j = 0; j < drow.length; j++) {
                trow.append($('<td>', {text: drow[j]}));
            }
            tbody.append(trow);
        }
    }
}

function populate_table(table_id, key, label) {
    console.log("Populating " + table_id + " table");
    var tbody = $('#' + table_id + ' > tbody');
    tbody.empty();
    if (!slowsql_data[key] || slowsql_data[key].length == 0) {
        var trow = $('<tr>', {id: label + "1"});
        trow.append($('<td>', {colspan: "6", id: label + "1rank", text: "No Data for " + key}));
        tbody.append(trow);
    } else {
        var maxRows = parseInt($('#filter_rowcount').find(":selected").val());
        for (var i = 0; i < slowsql_data[key].length; i++) {
            if (i >= maxRows) break;
            var rank = i + 1;
            var trow = $('<tr>', {id: label + rank});
            trow.append($('<td>', {id: label + rank + "rank", text: rank}));
            var drow = slowsql_data[key][i];
            for (var j = 0; j < drow.length; j++) {
                trow.append($('<td>', {text: drow[j]}));
            }
            tbody.append(trow);
        }
    }
}

function update_week_over_week(lastWeekKey, thisWeekKey) {
    var thisWeekQueryRank = {};
    var maxRows = parseInt($('#filter_rowcount').find(":selected").val());
    var end = maxRows;
    if (slowsql_data[thisWeekKey].length < end) {
        end = slowsql_data[thisWeekKey].length;
    }
    for (var i = 0; i < end; i++) {
        thisWeekQueryRank[slowsql_data[thisWeekKey][i][4]] = i+1;
    }

    var lastWeekQueryRank = {};
    end = maxRows;
    if (slowsql_data[lastWeekKey].length < end) {
        end = slowsql_data[lastWeekKey].length;
    }
    for (var i = 0; i < end; i++) {
        lastWeekQueryRank[slowsql_data[lastWeekKey][i][4]] = i+1;
    }

    var lastWeekKeys = Object.keys(lastWeekQueryRank);
    var thisWeekKeys = Object.keys(thisWeekQueryRank);
    for (var i = 0; i < lastWeekKeys.length; i++) {
        var key = lastWeekKeys[i];
        //console.log("Checking " + key);
        if (!thisWeekQueryRank[key]) {
            //console.log("missing in this week: " + key);
            $('#lw' + lastWeekQueryRank[key] + "> td").addClass("missing");
        } else if (thisWeekQueryRank[key] != lastWeekQueryRank[key]) {
            if(thisWeekQueryRank[key] < lastWeekQueryRank[key]) {
                //console.log("moved up this week: " + key);
                $('#tw' + thisWeekQueryRank[key] + "> td").addClass("up");

            } else if(thisWeekQueryRank[key] > lastWeekQueryRank[key]) {
                //console.log("moved down this week: " + key);
                $('#tw' + thisWeekQueryRank[key] + "> td").addClass("down");
            }
            var thisrank = thisWeekQueryRank[key];
            var lastrank = lastWeekQueryRank[key];
            var ranktext = thisrank + " (was " + lastrank + ")";
            $('#tw' + thisWeekQueryRank[key] + "rank").html(ranktext);
        //} else {
        //    console.log("no change: " + key);
        }
    }

    console.log("Looking for new queries this week");
    thisWeekKeys.forEach(function(key, idx, arr) {
        if (!lastWeekQueryRank[key]) {
            //console.log("new this week: " + key);
            $('#tw' + thisWeekQueryRank[key] + "> td").addClass("new");
        }
    });
}

/*
function populate_last_week(lastWeekKey) {
    console.log("Populating last week's data table");

    var thisWeekKey = "" + yyyymmdd(thisWeekStart) + "-" + yyyymmdd(thisWeekEnd);
    var tbody = $('#previous_data_table > tbody');
    tbody.empty();
    var lastWeekQueryRank = {};
    if (slowsql_data[lastWeekKey]) {
        if (slowsql_data[lastWeekKey].length == 0) {
            var trow = $('<tr>', {id: "lw1"});
            trow.append($('<td>', {colspan: "6", id: "lw1rank", text: "No Data for " + lastWeekKey}));
            tbody.append(trow);
        }
        var maxRows = parseInt($('#filter_rowcount').find(":selected").val());
        console.log("limiting to " + maxRows + " rows");
        for (var i = 0; i < slowsql_data[lastWeekKey].length; i++) {
            var rank = i + 1;
            if (i >= maxRows) break;
            var trow = $('<tr>', {id: "lw" + rank});
            trow.append($('<td>', {id: "lw" + rank + "rank", text: rank}));
            var drow = slowsql_data[lastWeekKey][i];
            for (var j = 0; j < drow.length; j++) {
                trow.append($('<td>', {text: drow[j]}));
            }
            lastWeekQueryRank[drow[4]] = rank;
            tbody.append(trow);
        }
    }

    console.log("Done with last week's data table... calculating ranks");

    // For each query, check it in "thisWeekKey" and update the changes accordingly.
    var thisWeekQueryRank = {};
    for (var i = 0; i < slowsql_data[thisWeekKey].length; i++) {
        thisWeekQueryRank[slowsql_data[thisWeekKey][i][4]] = i+1;
    }

    var lastWeekKeys = Object.keys(lastWeekQueryRank);
    var thisWeekKeys = Object.keys(thisWeekQueryRank);
    for (var i = 0; i < lastWeekKeys.length; i++) {
        var key = lastWeekKeys[i];
        //console.log("Checking " + key);
        if (!thisWeekQueryRank[key]) {
            //console.log("missing in this week: " + key);
            $('#lw' + lastWeekQueryRank[key] + "> td").addClass("missing");
        } else if(thisWeekQueryRank[key] < lastWeekQueryRank[key]) {
            //console.log("moved up this week: " + key);
            $('#tw' + thisWeekQueryRank[key] + "> td").addClass("up");
            var ranktext = $('#tw' + thisWeekQueryRank[key] + "rank").val();

        } else if(thisWeekQueryRank[key] > lastWeekQueryRank[key]) {
            //console.log("moved down this week: " + key);
            $('#tw' + thisWeekQueryRank[key] + "> td").addClass("down");
        } else {
            console.log("no change: " + key);
        }
    }

    console.log("Looking for new queries this week");
    thisWeekKeys.forEach(function(key, idx, arr) {
        if (!thisWeekQueryRank[key]) {
            //console.log("new this week: " + key);
            $('#tw' + thisWeekQueryRank[key] + "> td").addClass("new");
        }
    });
}*/

function get_slowsql_type() {
    return $('input[name=slowsql_type]:radio:checked').val();
}

function get_key(start, end) {
    return get_slowsql_type() + "_" + yyyymmdd(start) + "-" + yyyymmdd(end);
}

function update_data() {
    $('#current_data_header').html("This Week: " + yyyymmdd(thisWeekStart) + " to " + yyyymmdd(thisWeekEnd));
    // Update last week's header:
    $('#previous_data_header').html("Week Of: " + yyyymmdd(lastWeekStart) + " to " + yyyymmdd(lastWeekEnd));

    // Update this week's data if need be:
    var thisWeekKey = get_key(thisWeekStart, thisWeekEnd);
    var lastWeekKey = get_key(lastWeekStart, lastWeekEnd);
    // Load this week's data
    fetch_data(thisWeekKey, function(){
        fetch_data(lastWeekKey, function() {
            populate_table("current_data_table", thisWeekKey, "tw");
            populate_table("previous_data_table", lastWeekKey, "lw");
            //populate_this_week(thisWeekKey);
            //populate_last_week(lastWeekKey);
            update_week_over_week(lastWeekKey, thisWeekKey);
        });
    });

    // Update last week's data if need be:
    // var lastWeekKey = "" + yyyymmdd(lastWeekStart) + "-" + yyyymmdd(lastWeekEnd);
    // // Load last week's data
    // if (!slowsql_data[lastWeekKey]) {
    //     fetch_data(lastWeekKey, populate_last_week);
    // }
}

$(function () {
    $('#previous_week').click(function() {
        lastWeekStart.setDate(lastWeekStart.getDate() - 7);
        lastWeekEnd.setDate(lastWeekEnd.getDate() - 7);
        update_data();
    });
    $('#next_week').click(function() {
        lastWeekStart.setDate(lastWeekStart.getDate() + 7);
        lastWeekEnd.setDate(lastWeekEnd.getDate() + 7);
        update_data();
    });
    $('#filter_rowcount').change(update_data);
    $('input[name=slowsql_type]').change(update_data);

    update_data();
    $('#current_data_header').html("This Week: " + yyyymmdd(thisWeekStart) + " to " + yyyymmdd(thisWeekEnd));
    //$('#previous_data_header').html("Last Week: " + yyyymmdd(lastWeekStart) + " to " + yyyymmdd(lastWeekEnd));
    $(document).tooltip({delay: 1000});

});
