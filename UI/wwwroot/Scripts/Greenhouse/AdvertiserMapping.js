var checkedIds = {};

//var gridMapped;

$(document).ready(function () {

    // Disable "Save Mapped Advertiser(s)" Button
    $("#saveMapped.btn.btn-primary").prop('disabled', true);
    $("#saveMapped.btn.btn-primary").toggleClass('disabled', true);

    //Populate the Data Source Drop Down List.
    var categories = $("#datasource").kendoDropDownList({
        optionLabel: "Select Data Source...",
        dataTextField: "DataSourceName",
        dataValueField: "DataSourceID", //"GUID",
        dataSource: dsDataSource
    }).data("kendoDropDownList");

    //Populate the Instance Drop Down List when the DataSource is selected.
    var products = $("#instance").kendoDropDownList({
        optionLabel: "Select Instance...",
        dataTextField: "InstanceName",
        dataValueField: "InstanceID", //"GUID",
        dataSource: dsInstance
    }).data("kendoDropDownList");

    // ==================================================================================================================================================================

    //Check for changes on the Source Drop Down List, clear the Available and Mapped Advertisers tables.

    var refreshAdvertiserLists = function() {
        var datasource = $("#datasource").val();
        var instance = $("#instance").val();
        var isAggregate = $('#isAggregateFlag').prop('checked');

        if (datasource == null || datasource.length == 0 || instance == null || instance.length == 0) {
            $("#gridAvailable").data('kendoGrid').dataSource.data([]);
            $("#gridMapped").data('kendoGrid').dataSource.data([]);
        }
        else {
            fnShowLoading(true);

            getAllAdvertisers(datasource, instance, isAggregate);
        }
    }

    $(document).on("change", "#datasource", function () {
        refreshAdvertiserLists();
    });

    //Check for changes on the instance Drop Down List.
    $(document).on("change", "#instance", function () {
        refreshAdvertiserLists();
    });

    //Check for changes on the isAggregate checkbox
    $(document).on("change", "#isAggregateFlag", function () {
        refreshAdvertiserLists();
    });

    // ==================================================================================================================================================================

    function getAllAdvertisers(datasource, instance, isAggregate) {
        $.ajax({
            type: "POST",
            url: baseUrl + '/getAllAdvertisers',
            data: { DataSourceID: datasource, InstanceID: instance, isAggregate: isAggregate },
            dataType: "json",
            success: function (data) {
                //console.log(data);
                if (data) {
                    var normalTemplate = kendo.template("<tr data-uid='#: uid #'><td><input type='checkbox' class='checkbox' /></td><td>#: AdvertiserNameDisplay #</td></tr>");

                    var allNonMapped = data.filter(
                        function(el) {
                            return !el.IsMapped;
                        }
                    );

                    var dsNonMapped = new kendo.data.DataSource({ data: allNonMapped });
                    setTemplate(normalTemplate, dsNonMapped);

                    var allMapped = data.filter(
                        function (el) {
                            return el.IsMapped;
                        }
                    );

                    var dsAllMapped = new kendo.data.DataSource({ data: allMapped });
                    setMappedTemplate(normalTemplate, dsAllMapped);

                } else {
                    var ds = new kendo.data.DataSource({ data: [] });
                    $('#gridAvailable').data('kendoGrid').setDataSource(ds);
                    showMessage("No Advertisers Exists for this selections.");
                }
                fnShowLoading(false);
            },
            error: function (XMLHttpRequest, textStatus, errorThrown) {
                fnShowLoading(false);
                alert("The call retrieving the available advertisers failed");
            }
        });
    }

    // ==================================================================================================================================================================

    $(document).on("click", "#addToTrackedList", function () {
        var datasource = $("#datasource").val();
        var instance = $("#instance").val();

        if (instance == 0) {
            alert("You must select an Instance.");
            return;
        }

        var ds = $('#gridMapped').data("kendoGrid").dataSource;

        var data = ds.data();
        var message = "";

        //FOR ALL ADVERTISERS SELECTED
        for (var key in checkedIds) {
            //console.log(checkedIds[key]);

            var addRecord = true;
            for (var i = 0; i < data.length; i++) {
                if (data[i].InstanceID == instance && data[i].AdvertiserMappingID == key) {
                    message = "Duplicates are not allowed";
                    addRecord = false;
                }
            }
            if (addRecord) {

                var data2 = {
                    AdvertiserMappingID: Number(key),
                    InstanceID: Number(instance),
                    AdvertiserNameDisplay: checkedIds[key].AdvertiserNameDisplay,
                    AdvertiserName: checkedIds[key].AdvertiserName,
                    Flag: "new",
                    IsAggregate: true
                }

                // Add to grdMapped
                ds.add(data2);

                // Remove from gridAvailable
                var gridAvailable = $('#gridAvailable').data("kendoGrid");
                gridAvailable.dataSource.remove(checkedIds[key]);
                gridAvailable.dataSource.sync();

                // Remove from checkedIds
                delete checkedIds[key];


                // Enable "Save Mapped Advertiser(s)" Button
                $("#saveMapped.btn.btn-primary").prop('disabled', false);
                $("#saveMapped.btn.btn-primary").toggleClass('disabled', false);

            }
        }
    });

    // ==================================================================================================================================================================

    $(document).on("click", "#saveMapped", function () {

        var datasource = $("#datasource").val();
        var instance = $("#instance").val();
        var isAggregate = $('#isAggregateFlag').prop('checked');

        var ds = $('#gridMapped').data("kendoGrid").dataSource;
        var data = ds.data();

        var advertiserMappingIDs = "";
        for (var i = 0; i < data.length; i++) {
            if (data[i]["Flag"] == "new") {
                advertiserMappingIDs += data[i]["AdvertiserMappingID"] + ",";
            }
        }

        if (advertiserMappingIDs.length == 0) {
            alert("There are no Advertisers to map.");
            return;
        }

        $.ajax({
            type: "POST",
            url: baseUrl + '/SaveMapped',
            data: { advertiserMappingIDs: advertiserMappingIDs, InstanceID: instance },
            dataType: "json",
            success: function (data) {

                showConfirmationMessage("The mappings have been successfully saved.");

                getAllAdvertisers(datasource, instance, isAggregate);

                // Disable "Save Mapped Advertiser(s)" Button
                $("#saveMapped.btn.btn-primary").prop('disabled', true);
                $("#saveMapped.btn.btn-primary").toggleClass('disabled', true);
            },
            error: function (XMLHttpRequest, textStatus, errorThrown) { alert("some error"); }
        });
    });

    // ==================================================================================================================================================================

    //AVAILABLE ADVERTISERS GRID
    //gridAvailable definition
    var gridAvailable = $("#gridAvailable").kendoGrid({
        //dataSource: dsAdvertiser,        
        height: 400,
        filterable: {
            extra: false,
            operators: {
                string: {
                    eq: " Equals to",
                    contains: " Contains ",
                    startswith: " Starts With ",
                    neq: " Not Equal to "
                }
            },
            messages: {
                info: "Filter by",
                selectValue: "--Select--"
            }
        },
        filterMenuInit: function (e) {
            e.container.find(".k-widget").css("width", "320px")
        },
        columns: [
            { field: "Action", title: "<!---->", width: "30px", template: "<input type='checkbox' class='checkbox' />", filterable: false, },
            {
                editable: false,
                field: "AdvertiserNameDisplay",
                title: "Advertiser Name",
                filterable: {
                    ui: function filterAdvertiser(ele) {
                        var ds = FilterGridData('AdvertiserNameDisplay');

                        ele.kendoComboBox({
                            dataSource: ds
                            , placeholder: "--Type or Select an Advertiser--"

                        });
                    }
                }
            },
            { field: "IsAggregate", hidden: true }
        ],
        schema: {
            type: 'json',
            model: {
                id: "AdvertiserMappingID",
                fields: {
                    AdvertiserMappingID: { type: "string" },
                    AdvertiserName: { type: "string" }
                }
            }
        }
    }).data("kendoGrid");

    //bind click event to the checkbox
    gridAvailable.table.on("click", ".checkbox", selectRow);

    function FilterGridData(field) {
        var ds = $.unique(gridAvailable.dataSource.data().map(function getClients(obj, indx) {
            return obj[field];
        }).sort(function sortFilteredData(str1, str2) {
            var a = str1.toLowerCase(), b = str2.toLowerCase();
            return a < b ? -1 : a > b ? 1 : 0;
        }));
        return ds;
    }

    //MAPPED ADVERTISERS GRID
    //gridMapped definition
    var gridMapped = $("#gridMapped").kendoGrid({
        //autoBind: false,
        dataSource: MappedAdDataSource(),
        dataBound: onDataBound,
        height: 400,
        columns: [
            { name: "destroy", text: "delete", template: "<a class='btn-small lnkDelete'><i class='fa fa-times'></i></a>", width: "33px" },
            { field: "AdvertiserNameDisplay", title: "Advertiser Name", width: "320px", attributes: { id: 'CustomMessage' } },
            { field: "IsAggregate", hidden: true }
        ],
        editable: {
            update: false,
            destroy: false
        }
    }).data("kendoGrid");


    // Tooltip for AdvertiserName...
    var gridTT = $("#grid").kendoTooltip({
        filter: "#CustomMessage",
        position: "bottom",
        content: function (e) {
            var dataItem = gridMapped.dataItem(e.target.closest("tr"));
            var content = dataItem.AdvertiserNameDisplay;
            return content
        }
    }).data("kendoTooltip");

    function MappedAdDataSource() {
        return new kendo.data.DataSource({
            schema: {
                type: 'json',
                model: {
                    id: "AdvertiserMappingID",
                    fields: {
                        AdvertiserMappingID: { type: "string" },
                        InstanceID: { type: "string" },
                        AdvertiserName: { editable: false, type: "string" },
                        Flag: { type: "string" }
                    }
                }
            }
        });//end dataSource
    }//end DataSource()

    function onDataBound(e) {
        // Show Delete option only for Advertisers added from the Available grid

        //var grid = $("#gridMapped").data("kendoGrid");
        //var gridData = grid.dataSource.view();

        //for (var i = 0; i < gridData.length; i++) {
        //    var currentUid = gridData[i].uid;

        //    if (gridData[i].InstanceID != null && gridData[i].InstanceID != 0) {

        //        var currenRow = grid.table.find("tr[data-uid='" + currentUid + "']");
        //        var editButton = $(currenRow).find("a.btn-small.lnkDelete");

        //        editButton.hide();
        //    }
        //}
    }


    $("#gridMapped").on("click", ".lnkDelete", function (e) {

        if (!confirm("Are you sure you wish to delete this?"))
            return;

        let lyrInstance = $("#instance");
        var instance = lyrInstance.val();
        var datasource = $("#datasource").val();
        var isAggregate = $('#isAggregateFlag').prop('checked');

        var row = $(this).closest("tr");
        var grid = $("#gridMapped").data("kendoGrid");

        dataItem = grid.dataItem(row);

        var removedAdvertiserMappingID = dataItem.AdvertiserMappingID;
        var removedAdvertiserName = dataItem.AdvertiserName;
        var alreadymappedAdvertiser = true;

        // Just moved over from the AVAILABLE grid but prior to being saved.
        if (dataItem.Flag == "new")
            alreadymappedAdvertiser = false;

        if (alreadymappedAdvertiser) {
            $.ajax({
                type: "POST",
                url: baseUrl + '/DeleteMappedAdvertiser',
                data: { InstanceID: instance, advertiserMappingID: removedAdvertiserMappingID },
                dataType: "json",
                success: function () {
                    fnShowLoading(true);
                    getAllAdvertisers(datasource, instance, isAggregate);
                },
                error: function (XMLHttpRequest, textStatus, errorThrown) {
                    alert("error deleting...");
                }
            });
        }

        // Remove from gridMapped
        grid.removeRow(row);
        grid.dataSource.remove(dataItem);
        grid.dataSource.sync();

        //// Add back to gridAvailable
        //var gridAvailable = $('#gridAvailable').data("kendoGrid");
        //gridAvailable.dataSource.add({ AdvertiserMappingID: removedAdvertiserMappingID, AdvertiserName: removedAdvertiserName });
    	//gridAvailable.dataSource.sync();
    	//refresh gridAvailable
        if (!alreadymappedAdvertiser) {
            lyrInstance.trigger("change");
        }

        // The "Save Mapped Advertiser(s)" Button should be disabled when there are no newly mapped advertisers.
        var noNewMaps = true;
        var data = grid.dataSource.data();

        for (var i = 0; i < data.length; i++) {
            if (data[i]["Flag"] == "new") {
                noNewMaps = false;
                break;
            }
        }

        if (noNewMaps)
        {
            // Disable "Save Mapped Advertiser(s)" Button
            $("#saveMapped.btn.btn-primary").prop('disabled', true);
            $("#saveMapped.btn.btn-primary").toggleClass('disabled', true);
        }

    });
   
});

    // ==================================================================================================================================================================

    
function setTemplate(newTemplate, ds) {
    $('#gridAvailable').data('kendoGrid').options.rowTemplate = newTemplate;
    $('#gridAvailable').data('kendoGrid').setDataSource(ds);
}

function setMappedTemplate(newTemplate, ds) {
    $('#gridMapped').data('kendoGrid').options.rowTemplate = newTemplate;
    $('#gridMapped').data('kendoGrid').setDataSource(ds);
}


//on click of the checkbox:
function selectRow() {
    var checked = this.checked,
    row = $(this).closest("tr"),
    gridAvailable = $("#gridAvailable").data("kendoGrid"),
    dataItem = gridAvailable.dataItem(row);
    //console.log(dataItem);

    if (checked) {
        //-select the row
        row.addClass("k-state-selected");
        checkedIds[dataItem.AdvertiserMappingID]= dataItem;
    } else {
        //-remove selection
        row.removeClass("k-state-selected");
        delete checkedIds[dataItem.AdvertiserMappingID];
    }
}

function showMessage(message) {
    if(message.length > 0) {
        $("#stallion-message").show();
        $("#messageText").text(message);
        setTimeout(function() {
            $("#stallion-message").hide();
        }, 4000);
    }
}

function showConfirmationMessage() {
    $("#stallion-confirmation").show();
    setTimeout(function () {
        $("#stallion-confirmation").hide();
    }, 4000);
}

function showAggregateSource() {
    $("#gridAvailable").data('kendoGrid').dataSource.filter({
        field: "IsAggregate",
        operator: "eq",
        value: true
    });
    $("#gridMapped").data('kendoGrid').dataSource.filter({
        field: "IsAggregate",
        operator: "eq",
        value: true
    });
}