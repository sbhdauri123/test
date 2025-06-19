    var gridElement, jobRunGrid;
    var tplyesno = kendo.template($("#tplYesNo").html());
    var tplstatus = kendo.template($("#tplStatus").html());
    var url = "/jobRunHub";

    var jobRunHub = new signalR.HubConnectionBuilder()
        .withUrl(url, {
            transport: signalR.HttpTransportType.LongPolling
        })
        .build();

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

        var gridDataSource = GetJobRunDataSource();
        gridDataSource.bind("error", OnDataSourceError);	
        gridElement = $("#jobRunGrid");

        jobRunGrid = gridElement.kendoGrid({
            dataSource: gridDataSource,
            dataBound: onDataBound,
            height: 600,
            sortable: { mode: "single", allowUnsort: true },
            pageable: { refresh: true },
            resizable: true,
            columns: [
                {
                    command: [
                        { name: "destroy", text: "delete", template: "<a class='btn-small k-grid-delete'><i class='fa fa-times grid-edit-link' title='delete'></i></a>", width: 10 }
                    ]
                    , width: 60
                    , menu: false
                }
                //,{ field: "JobRunId", title: "Log ID", width: "120px" }            
                , { field: "Advertiser", title: "Advertiser", width: "300px", filterable: { ui: advertiserFilter } }            
                , { field: "Status", title: "Status", width: "110px", template: '#=fnTemplateStatus(Status)#', filterable: { ui: statusFilter } }
                , { field: "PlacementsModified", title: "Placements Updated", width: "200px", filterable: false }
                //, { field: "StartPlacementId", title: "Start ID", width: "120px" }
                , { field: "DisplayLastPlacementId", title: "Last Placement ID", width: "180px", filterable: false }
                , { field: "DisplayHasuValueError", title: "uValue Error?", width: "150px", filterable: { ui: hasuValueErrorFilter } }
                , { field: "ExecutionTime", title: "Exec. Time", width: "130px", filterable: false }
                , { field: "LastUpdated", title: "Complete Time", width: "200px", template: "#=kendo.toString(LastUpdated, 'G')#", filterable: false }
                , { field: "Message", title: "Message", width: "200px", template: "#= trimText(Message, 20) #", attributes: { id: 'CustomMessage'}, filterable: false }
            ]
            , editable: {
                mode: "popup"
                , edit: true
                , destroy: true
                , create: false
                , confirmation: "Are you sure you want to delete this?"
            }
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

        function onDataBound(e) {

            var grid = $("#jobRunGrid").data("kendoGrid");
            var gridData = grid.dataSource.view();

            for (var i = 0; i < gridData.length; i++) {
                var currentUid = gridData[i].uid;

                if (gridData[i].FileLogCount != null && gridData[i].FileLogCount == 0) {

                    var currenRow = grid.table.find("tr[data-uid='" + currentUid + "']");
                    var editButton = $(currenRow).find("a.k-button.k-button-icontext.k-grid-details");

                    editButton.hide();
                }
            }
        }

        fnTemplateYesNo = function (value) {
            var htm = tplyesno({
                Val: value
            });
            return htm;
        };

        fnTemplateStatus = function (value) {
            var htm = tplstatus({
                Val: value
            });
            return htm;
        };
        

        var gridTT= $("#grid").kendoTooltip({
            //filter: "td:nth-child(8)",
            show: function (e) {
                if (this.content.text() != null && this.content.text().length > 0) {
                    this.content.parent().css("visibility", "visible");
                }
            },
            hide: function (e) {
                this.content.parent().css("visibility", "hidden");
            },
            filter: "#CustomMessage",
            position: "bottom",
            content: function (e) {
                var dataItem = jobRunGrid.dataItem(e.target.closest("tr"));
                var content = dataItem.Message;
                return content
            }
        }).data("kendoTooltip");
			

        $("#jobRunContainer").on("change", "#autoRefreshSelect", function setRefreshGridTimer() {
            var interval = $(this).val();

            var timer = (function getTimer() {
                this.timer = this.timer || new autoRefresh(jobRunGrid, interval);

                return this.timer;
            })();

            timer.resetAutoRefresh(interval);
        });

        $("#autoRefreshSelect").val("30").change();

        function OnDataSourceError(e) {
            if (!$("#dialog").data("kendoWindow")) {
                $('#dialog').kendoWindow({
                    title: "Error",
                    modal: true,
                    height: 200,
                    width: 400
                });
            }
            var dialog = $("#dialog").data("kendoWindow");
            dialog.content(e.xhr.responseText);
            dialog.center();
            dialog.open();
            jobRunGrid.cancelChanges();
        }
	
        function GetJobRunDataSource() {
            return new kendo.data.DataSource({
                type: "signalr",
                autoSync: false,
                transport: {
                    signalr: {
                        promise: jobRunHub.start(),
                        hub: jobRunHub,
                        server: {
                            read: "readAll"
                            , destroy: "destroy"
                        },
                        client: {
                            read: "readAll"
                            , destroy: "destroy"
                        }
                    }
                }                
                , error: function (e) {
                    //http://www.telerik.com/forums/rows-disappeared-from-grid-on-delete-fail
                    jobRunGrid.data("kendoGrid").cancelChanges();
                    alert("Error...");
                }
                , schema: {
                    model: {
                        id: "JobRunId",
                        fields: {
                            JobRunId: { type: "number" }
                            , Advertiser: { type: "string" }
                            , DisplayLastPlacementId: { type: "string" }
                            , DisplayHasuValueError: { type: "string" }
                            , StartPlacementId: { type: "number" }
                            , Status: { type: "string" }
                            , StatusSortOrder: { type: "number" }
                            , HasuValueError: { type: "boolean" }
                            , Message: { type: "string" }
                            , PlacementsModified: { type: "number" }
                            , CreatedDate: { type: "date", format: "MM/dd/yyyy", parse: function (value) { return parseUTCDate(value) } }
                            , LastUpdated: { type: "date", format: "MM/dd/yyyy", parse: function (value) { return parseUTCDate(value) } }
                        }
                    }
                }
               , requestEnd: function (e) {
                   kendo.ui.progress(gridElement, false);
               }
               , requestStart: function () {
                   //kendo.ui.progress(gridElement, true)
               }
               , serverPaging: false
               , serverSorting: false
               , serverFiltering: false
               , pageSize: 25
            });
        }//end DataSource()


        function advertiserFilter(element) {
            var ds = filterGridData('Advertiser');

            element.kendoComboBox({
                dataSource: ds
                , placeholder: "--Select an Advertiser--"

            });
        }

        function statusFilter(element) {
            var ds = filterGridData('Status');

            element.kendoComboBox({
                dataSource: ds
                , placeholder: "--Select a Status--"

            });
        }

        function hasuValueErrorFilter(element) {
            var ds = filterGridData('DisplayHasuValueError');

            element.kendoComboBox({
                dataSource: ds
                , placeholder: "--Select has uValue Error--"

            });
        }

        function filterGridData(field) {
            var ds = $.unique(jobRunGrid.dataSource.data().map(function getValues(obj, indx) {
                return obj[field];
            }).sort(function sortFilteredData(str1, str2) {
                var a, b;
                if (Boolean(str1) && Boolean(str2)) {
                    a = str1.toLowerCase(); b = str2.toLowerCase();
                }
                else { a = ""; b = ""; }
                return a < b ? -1 : a > b ? 1 : 0;
            }));
            return ds;
        }

});

