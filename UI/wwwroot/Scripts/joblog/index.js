
    var gridElement, jobLogGrid;
    
    $(document).ready(function () {

        var dialog = $("#dialog").kendoWindow({
            modal: true,
            visible: false,
            title: "View File Log"
        }).data("kendoWindow");

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

        var gridDataSource = GetJobLogDataSource();
        gridDataSource.bind("error", OnDataSourceError);	
        gridElement = $("#jobLogGrid");

        jobLogGrid = gridElement.kendoGrid({
            dataSource: gridDataSource,
            dataBound: onDataBound,
            height: 600,
            sortable: { mode: "single", allowUnsort: true },
            pageable: { refresh: true },
            resizable: true,
            columns: [
                {
                    command: [{ name: "destroy", template: "<a class='btn-small k-grid-delete'><i class='grid-edit-link glyphicon glyphicon-remove' title='delete job log'></i></a> ", text: " ", width: 10 }
                    ], width: 50, menu: false
                }
                //, { hidden: true, field: "JobLogID", title: "JobLogID" }
                , { field: "SourceName", title: "Source", width: "150px", filterable: { ui: sourceFilter }  }
                , { field: "JobDescription", title: "Step", width: "160px", filterable: { ui: stepFilter } }
                , { field: "JobStatus", title: "Status", template: kendo.template($("#StatusTemplate").html()), width: "100px", filterable: { ui: statusFilter }  }
                , { field: "IntegrationName", title: "Integration", width: "200px", filterable: { ui: integrationFilter }  }
                , { field: "ExecutionTime", title: "Exec. Time", width: "130px", filterable: false }
                , { field: "Message", title: "Message", width: "200px", attributes: { id: 'CustomMessage', style: 'white-space: nowrap ' }, filterable: false }
                , { field: "LastUpdated", title: "Last Updated", width: "175px", format: "{0:G}", filterable: false }
                , { command: { name: "details", text: " ", iconClass: "fa fa-search-plus", click: showDetails }, title: " ", width: "57px" }
                , { field: "JobGUID", title: " ", width: "75px", filterable: false, template: kendo.template($("#JobGUIDTemplate").html()) }
                
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

            var grid = $("#jobLogGrid").data("kendoGrid");
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


        // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        // JOB LOG MODAL CODE

        var wnd, detailsTemplate;

        wnd = $("#details")
            .kendoWindow({
                title: "File Logs",
                modal: true,
                visible: false,
                resizable: false,
                width: 900,
                open: onOpen
        }).data("kendoWindow");

        function onOpen(e) {
            this.wrapper.css({ top: 125 });
        }

        detailsTemplate = kendo.template($("#template").html());

        function showDetails(e) {
            e.preventDefault();

            var dataItem = this.dataItem($(e.currentTarget).closest("tr"));
            wnd.content(detailsTemplate(dataItem));
            wnd.center().open();

            var modalGrid = $("#modalgrid").kendoGrid({
                dataSource: {
                    type: "json",
                    transport: {
                        read: "/JobLog/GetFileLogsPerJob/" + dataItem.JobLogID
                    },
                    schema: {
                        model: {
                            fields: {
                                JobLogID: {type: "number"},
                                FileName: { type: "string" },
                                Status: { type: "string"},
                                LastUpdated: { type: "date", parse: function (value) { return parseUTCDate(value) } },
                            }
                      }
                    },
                    serverPaging: false,
                    serverFiltering: false,
                    serverSorting: false,
                    pageSize: 10
              },
              filterable: true,
              sortable: true,
              pageable: true,
              resizable: true,
              scrollable: true,
              columns: [
                    { field: "FileName", title: "File Name", width: 375, attributes: { id: 'CustomMessageModal' } },
                    { field: "Status", title: "Status", width: 130 },
                  { field: "LastUpdated", title: "Last Updated", width: 130, format: "{0:MM/dd/yyyy hh:mm:ss}" }
              ]
              
            });


            $("#autoRefreshSelectModal").val("30").change();


            $("#modalgrid").kendoTooltip({
                filter: "td:nth-child(1)",
                position: "bottom",
                content: function (e) {
                    var dataItem = $("#modalgrid").data("kendoGrid").dataItem(e.target.closest("tr"));
                    var content = dataItem.FileName;
                    return content;
                }
            }).data("kendoTooltip");


            $("#details-container").on("change", "#autoRefreshSelectModal", function setRefreshGridTimerModal() {
                var interval = $(this).val();

                var timer = (function getTimerModal() {

                    //this.timer = this.timer || new autoRefresh(modalGrid, interval);
                    //this.timer = new autoRefreshModal(modalGrid, interval);
                    this.timer = new autoRefreshModal($("#modalgrid").data("kendoGrid"), interval);

                    return this.timer;
                })();

                timer.resetAutoRefresh(interval);
            });


            var autoRefreshModal = (function gridRefresh(kGrid, seconds) {
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
        }
        

        // JOB LOG MODAL CODE
        // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
        // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


        var gridTT= $("#grid").kendoTooltip({
            //filter: "td:nth-child(9)",
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
                var dataItem = jobLogGrid.dataItem(e.target.closest("tr"));
                var content = dataItem.Message;
                return content
            }
        }).data("kendoTooltip");
			

        $("#jobLogContainer").on("change", "#autoRefreshSelect", function setRefreshGridTimer() {
            var interval = $(this).val();

            var timer = (function getTimer() {
                this.timer = this.timer || new autoRefresh(jobLogGrid, interval);

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
            if (e.xhr.responseText === undefined)
                location.reload(true);
            else {
                dialog.open();
                jobLogGrid.cancelChanges();
            }
        }
	
        function GetJobLogDataSource() {
            return new kendo.data.DataSource({
                transport: {
                    read: {
                        url: "/JobLog/GetAllJobLogs"
                        , type: "GET"
                        , dataType: "json"
                        , complete: function (jqXHR, textStatus) {
                            //kendo.ui.progress(gridElement, false);
                        }
                    }
                    , destroy: {
                        url: "/JobLog/DeleteLog"
                       , type: "POST"
                       , dataType: "json"
                       , complete: function (jqXHR, textStatus) {
                           //kendo.ui.progress(gridElement, false);
                           if (jqXHR.responseJSON && jqXHR.responseJSON.LogCount)
                            jobLogGrid.dataSource.add(jqXHR.responseJSON);
                           
                       }
                    },
                    parameterMap: function(option, type) {
                        if (type === "destroy") {
                            option.Message = JSON.stringify(escape(option.Message));
                        }
                        return option;
                    }
                }
                , error: function (xhr, error) {
                    console.debug(xhr);
                    console.debug(error);
                }
               , schema: {
                   model: {
                       id: "JobLogID"
                       , fields: {
                          JobLogID: { type: "number" }
                        , SourceName: { type: "string" }
                        , IntegrationName: { type: "string" }
                        , JobStatus: { type: "string" }
                        , StepDescription: { type: "string" }
                        , StartDateTime: { type: "date" }
                           , LastUpdated: { type: "date", parse: function (value) { return parseUTCDate(value) } }
                        , ExecutionTime: { type: "string'" }
                        , Message: { type: "string"}
                        , JobGUID: { type: "string" }
                        , FileLogCount: { type: "number" }
                        , SplunkIndex: { type: "string" }
                        , LogCount: { type: "number" }

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
                   //kendo.ui.progress(gridElement, true)
               }
               , serverPaging: false
               , serverSorting: false
               , serverFiltering: false
               , pageSize: 25
            });
        }//end DataSource()


        function stepFilter(element) {
            var ds = filterGridData('StepDescription');

            element.kendoComboBox({
                dataSource: ds
                , placeholder: "--Select a Step--"

            });
        }

        function statusFilter(element) {
            var ds = filterGridData('JobStatus');

            element.kendoComboBox({
                dataSource: ds
                , placeholder: "--Select a Status--"

            });
        }

        function sourceFilter(element) {
            var ds = filterGridData('SourceName');

            element.kendoComboBox({
                dataSource: ds
                , placeholder: "--Select a Source--"

            });
        }

        function integrationFilter(element) {
            var ds = filterGridData('IntegrationName');

            element.kendoComboBox({
                dataSource: ds
                , placeholder: "--Select a Integration--"

            });
        }

        function filterGridData(field) {
            var ds = $.unique(jobLogGrid.dataSource.data().map(function getValues(obj, indx) {
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

