
var gridElement, jobQueueGrid;

$(document).ready(function () {

    var autoRefresh = (function gridRefresh(kGrid, seconds) {
        var autoRefreshTimer = null;
        var interval = seconds;

        function startAutoRefresh() {
            if (interval != null && interval > 0) {
                autoRefreshTimer = window.setTimeout(function () {
                    kGrid.dataSource.read();
                    startAutoRefresh();
                }, interval * 1000);
            }
        }

        function stopAutoRefresh() {
            if (autoRefreshTimer != null) {
                window.clearTimeout(autoRefreshTimer);
                autoRefreshTimer = null;
            }
        }

        function resetAutoRefresh(seconds) {
            stopAutoRefresh();
            interval = seconds;
            if (interval != null && interval > 0) {
                startAutoRefresh();
            }
        }

        return {
            stopAutoRefresh: stopAutoRefresh,
            startAutoRefresh: startAutoRefresh,
            resetAutoRefresh: resetAutoRefresh
        }

    });

    var gridDataSource = GetJobQueueDataSource();
    gridElement = $("#jobQueueGrid");

    jobQueueGrid = gridElement.kendoGrid({
        dataSource: gridDataSource,
        height: 600,
        sortable: { mode: "multiple", allowUnsort: true },
        pageable: { refresh: true },
        resizable: true,
        columns: [
            { field: "SourceName", title: "Source", width: "150px", filterable: { ui: sourceFilter } }
            , { field: "IntegrationName", title: "Integration", width: "200px", filterable: { ui: integrationFilter } }
            , { field: "EntityName", title: "Entity Name", width: "160px", filterable: { ui: entityFilter } }
            , { field: "EntityID", title: "Entity ID", width: "160px", filterable: { ui: entityIDFilter } }
            , { field: "IsBackFill", title: "IsBackfill", width: "160px", filterable: true }
            , { field: "Step", title: "Step", width: "160px", filterable: { ui: stepFilter } }
            , { field: "Status", title: "Status", width: "160px", filterable: false, template: kendo.template($("#StatusTemplate").html()), filterable: { ui: statusFilter } }
            , { field: "FileDate", title: "Data Date", width: "160px", template: "#= FileDateString#", filterable: false }
            , { field: "FileDateHour", title: "Data Date Hour", width: "160px", filterable: false }
            , { field: "DeliveryFileDate", title: "Delivery Date", width: "160px", template: "#= DeliveryFileDateString#", filterable: false }
            , { field: "FileSize", title: "FileSize", width: "160px", filterable: false }
            , { field: "FileName", title: "FileName", width: "160px", attributes: { id: 'CustomFileName', style: 'white-space: nowrap ' }, filterable: false }
            , { field: "FileGUID", title: "FileGUID", width: "160px", filterable: false }
            , { field: "LastUpdated", title: "Last Updated", width: "175px", template: "#=kendo.toString(LastUpdated, 'G')#", filterable: false }
        ]
        , filterable: {
            extra: false,
            operators: {
                string: {
                    contains: "Contains",
                    eq: "Is equal to",
                    neq: "Is not equal to"
                }
            }
        }

    }).data("kendoGrid");

    gridElement.kendoTooltip({
        filter: "#CustomFileName",
        position: "bottom",
        content: function (e) {
            var dataItem = jobQueueGrid.dataItem(e.target.closest("tr"));
            var content = dataItem.FileName;
            return content;
        }
    }).data("kendoTooltip");

    var currentUTC = kendo.timezone.apply(new Date(), "Etc/UTC");
    currentUTC.setHours(0, 0, 0, 0);

    var ddlItems = [
        { id: 1, label: "Today", filterDate: currentUTC },
        { id: 2, label: "Last 2 Days", filterDate: kendo.date.addDays(currentUTC, -1) },
        { id: 3, label: "Last 7 Days", filterDate: kendo.date.addDays(currentUTC, -7) },
        { id: 4, label: "Last 14 Days", filterDate: kendo.date.addDays(currentUTC, -14) }
    ];

    $('#ddl').kendoDropDownList({
        autoBind: true,
        dataTextField: "label",
        dataValueField: "filterDate",
        dataSource: {
            data: ddlItems
        },
        index: 0,
        change: function (e) {
            var value = kendo.toString(new Date(e.sender.value()), "s");
            jobQueueGrid.dataSource.filter({});
            jobQueueGrid.dataSource.options.transport.read.data.startDate = value;
            //HACK: Grid column filterMenuUI would not rebind unless the datasource is set. Don't know why, but it works. Dont ask.
            jobQueueGrid.setDataSource(jobQueueGrid.dataSource);
            jobQueueGrid.dataSource.read();                        
            jobQueueGrid.dataSource.page(1);
        }
    });

    var autoRefreshIntervals = [
        { label: "Off", seconds: 0 },
        { label: "Every 10 minutes", seconds: 600 },
        { label: "Every 30 minutes", seconds: 1800 },
        { label: "Every hour", seconds: 3600 }
    ];

    $('#ddlAutoRefresh').kendoDropDownList({
        autoBind: true,
        dataTextField: "label",
        dataValueField: "seconds",
        dataSource: {
            data: autoRefreshIntervals
        },
        index: 1,
        change: function (e) {
            var interval = e.sender.value();

            var timer = (function getTimer() {
                this.timer = this.timer || new autoRefresh(jobQueueGrid, interval);

                return this.timer;
            })();

            timer.resetAutoRefresh(interval);
        }
    });

    var ddlRefresh = $("#ddlAutoRefresh").data("kendoDropDownList");
    ddlRefresh.select(1);
    ddlRefresh.trigger("change");

    function GetJobQueueDataSource() {
        var currentUTC = kendo.timezone.apply(new Date(), "Etc/UTC");
        currentUTC.setHours(0, 0, 0, 0)
        var defaultStartDate = kendo.toString(currentUTC, "s");

        return new kendo.data.DataSource({
            transport: {
                read: {
                    url: "/JobQueue/GetAllQueueLogs"
                    , type: "GET"
                    , data: { startDate: defaultStartDate }
                    , contentType: "application/json; charset=utf-8"
                    , complete: function (jqXHR, textStatus) {
                    }
                }
            }
            , schema: {
                model: {
                    fields: {
                        SourceName: { type: "string" }
                        , IntegrationName: { type: "string" }
                        , EntityName: { type: "string" }
                        , EntityID: { type: "string" }
                        , IsBackFill: { type: "boolean" }
                        , Step: { type: "string" }
                        , Status: { type: "string" }
                        , FileDate: { type: "date", format: "MM/dd/yyyy" }
                        , FileDateString: { type: "string"}
                        , FileDateHour: { type: "number" }
                        , DeliveryFileDate: { type: "date", format: "MM/dd/yyyy" }
                        , DeliveryFileDateString: { type: "string" }
                        , FileSize: { type: "number" }
                        , FileName: { type: "string" }
                        , FileGUID: { type: "string" }
                        , LastUpdated: { type: "date", format: "MM/dd/yyyy", parse: function (value) { return parseUTCDate(value) } }
                        , LastUpdatedString: { type: "string" }
                    }
                }
                , total: function (data) {
                    return data.length;
                }
            }
            , requestEnd: function (e) {
                kendo.ui.progress(gridElement, false);
            }
            , requestStart: function () {
            }
            , serverPaging: false
            , serverSorting: false
            , serverFiltering: false
            , pageSize: 25
        });
    }//end DataSource()


    function stepFilter(element) {
        let dsStep = filterGridData('Step');

        element.kendoComboBox({
            dataSource: dsStep
            , placeholder: "--Select a Step--"

        });
    }

    function statusFilter(element) {
        let dsStatus = filterGridData('Status');

        element.kendoComboBox({
            dataSource: dsStatus
            , placeholder: "--Select a Status--"

        });
    }

    function sourceFilter(element) {
        let dsSource = filterGridData('SourceName');

        element.kendoComboBox({
            dataSource: dsSource
            , placeholder: "--Select a Source--"

        });
    }

    function integrationFilter(element) {
        let dsIntegration = filterGridData('IntegrationName');

        element.kendoComboBox({
            dataSource: dsIntegration
            , placeholder: "--Select a Integration--"

        });
    }

    function entityFilter(element) {
        let dsEntity = filterGridData('EntityName');

        element.kendoComboBox({
            dataSource: dsEntity
            , placeholder: "--Select an Entity--"

        });
    }


    function entityIDFilter(element) {
        let dsEntityId = filterGridData('EntityID');

        element.kendoComboBox({
            dataSource: dsEntityId
            , placeholder: "--Select an EntityID--"

        });
    }


    function filterGridData(field) {
        let ds = $.unique(jobQueueGrid.dataSource.data().map(function getValues(obj, indx) {
            return obj[field];
        }).sort(function sortFilteredData(str1, str2) {
            let a, b;
            if (Boolean(str1) && Boolean(str2)) {
                a = str1.toLowerCase(); b = str2.toLowerCase();
            }
            else { a = ""; b = ""; }
            return a < b ? -1 : a > b ? 1 : 0;
        }));
        return ds;
    }

    $("#clearFilterButton").click(function () {

        /*
        The code below will clear all filters from the
        Kendo Grid's DataSource by replacing it with an empty object.
        */
        jobQueueGrid.dataSource.filter({});
    });
});

