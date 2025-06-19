$(document).ready(function () {
    var userGrid, lyrUserGrid, manageUserHub;
    lyrUserGrid = $("#userGrid");
    var url = "/manageUserHub";

    manageUserHub = new signalR.HubConnectionBuilder()
        .withUrl(url, {
            transport: signalR.HttpTransportType.LongPolling
        })
        .build();
    async function start() {
        try {
            await manageUserHub.start();
            console.log("manageUserHub Connected.");
        } catch (err) {
            console.log(err);
            setTimeout(start, 5000);
        }
    };

    manageUserHub.onclose(async () => {
        await start();
    });

    // Start the connection.
    var hubStart = start();

    userGrid = lyrUserGrid.kendoGrid({
        dataSource: UsersDataSource(),
        columns: [
            {
                command: [{ name: "edit", text: "", template: "<a class='btn-small lnkEdit'><i class='fa fa-pencil-square-o grid-edit-link'></i></a>", width: 10 }
                    , { name: "destroy", template: "<a class='btn-small lnkDelete'><i class='fa fa-times grid-edit-link'></i></a>", text: "", width: 10 }], width: "135px"
                , title: "<button id='btnAdd' class='btn btn-primary'>Add User</button>", menu: false
            },
            { field: "UserID", title: "User ID" },
            { field: "Advertisers", title: "Mapped Advertisers", attributes: { "class": "withModal" }, template: "#= trimText(Advertisers, 20) #" },
            { field: "Instances", title: "Mapped Instances" }
        ],
        sortable: true,
        pageable: true,
        edit: EditUser,
        save: SaveUser,
        dataBound:  OnGridDataBind,
        editable: {
            mode: "popup"
            , edit: true
            , destroy: true
            , create: true
            , confirmation: "Are you sure you want to delete this?"
            , template: $("#editTemplate").html()
        },
        filterable: gridFilterOptions
    }).getKendoGrid();

    // ========================================================
    //THE FOLLOWING REFRESHES THE GRID UPON SAVE, BY RE-READING
    var grid = $("#userGrid").data("kendoGrid");
    grid.bind("save", onGridSave);

    function onGridSave(e) {
        e.sender.one("dataBound",
            function () {
                e.sender.dataSource.read();
            });
    };
    // ========================================================

    var dataSources, advertisers, instances;


    dataSources = new kendo.data.DataSource({
        transport: {
            type: "get",
            read: "GetDataSources"
        },
        schema: {
            model: {
                id: "DataSourceID"
                , fields: {
                    DataSourceID: { type: "string" },
                    DataSourceName: { type: "string" }
                }
            }
        },
        sort: { field: "DataSourceName", dir: "asc" }

    });
    dataSources.read();

    advertisers = new kendo.data.DataSource({
        transport: {
            type: "get",
            read: "GetAdvertisers"
        },
        schema: {
            model: {
                id: "AdvertiserMappingID"
                , fields: {
                    AdvertiserMappingID: { type: "string" },
                    AdvertiserNameDisplay: { type: "string" }
                }
            }
        },
        sort: { field: "AdvertiserNameDisplay", dir: "asc" }
    });
    advertisers.read();

    instances = new kendo.data.DataSource({
        transport: {
            type: "get",
            read: "GetInstances"
        },
        schema: {
            model: {
                id: "InstanceID"
                , fields: {
                    InstanceID: { type: "string" },
                    InstanceName: { type: "string" }
                }
            }
        },
        sort: { field: "InstanceName", dir: "asc" }
    });
    instances.read();

    var dsRedshiftUsers = new kendo.data.DataSource({
        transport: {
            type: "get",
            read: "GetRedshiftUsersUnmapped"
        },
        schema: {
            model: {
                id: "UserName"
                , fields: {
                    UserName: { type: "string" }
                }
            }
        },
        sort: { field: "UserName", dir: "asc" }
    });
    dsRedshiftUsers.read();

    $("table").kendoTooltip({
        filter: "td.withModal",
        position: "top",
        content: function (e) {
            var dataItem = userGrid.dataItem(e.target.closest("tr"));
            return dataItem.Advertisers;
        }
    }).data("kendoTooltip");

    function EditUser(evt) {
        let height = window.parent.innerHeight * .8;
        let kWindow = evt.container.getKendoWindow();
        let tbUserID = evt.container.find("#tbUserID");
        evt.container.width(1200)
        evt.container.height(height);

        kWindow.center();

        if (evt.model.isNew()) {
            kWindow.title("Add New User");
            tbUserID.prop('readonly', false);
        }
        else {
            kWindow.title("Editing User [" + evt.model.UserID + "]");
            tbUserID.prop('readonly', true);
        }

        /* remove default layout and behaviour of kendo window buttons*/
        let updateButton = evt.container.find(".k-grid-update");
        //updateButton.removeClass("k-grid-update");
        evt.container.find(".k-edit-buttons").removeClass("k-edit-buttons");


        RenderAdvertisers(evt.model.UserID);
        RenderInstances(evt.model.UserID);
        SetValidation(evt.container);
    }

    function SaveUser(evt) {

        let validator = evt.container.find("#lyrEdit").data("kendoValidator");
        if (!validator.validate()) {
            evt.preventDefault();
        }
        else {
            let selInstances = evt.container.find("#selectedInstances").getKendoListBox();
            let selAdvertiser = evt.container.find("#selectedAdvertisers").getKendoListBox();

            let instanceNames = selInstances.dataSource.data().map(i => i.InstanceName).join(",");
            let instanceIDs = selInstances.dataSource.data().map(i => i.InstanceID).join(",");
            let advertiserNames = selAdvertiser.dataSource.data().map(i => i.AdvertiserNameDisplay).join(",");
            let advertiserIDs = selAdvertiser.dataSource.data().map(i => i.AdvertiserMappingID).join(",");

            evt.model.set("Instances", instanceNames);
            evt.model.set("InstanceIDS", instanceIDs);
            evt.model.set("Advertisers", advertiserNames);
            evt.model.set("AdvertiserIDS", advertiserIDs);
        }
    }


    function UsersDataSource() {
        return new kendo.data.DataSource({
            type: "signalr"
            , autoSync: false
            , transport: {
                signalr: {
                    promise: hubStart,
                    hub: manageUserHub,
                    server: {
                        read: "readAll"
                        , create: "createUser"
                        , destroy: "destroy"
                        , update: "updateUser"
                    },
                    client: {
                        read: "read"
                        , create: "create"
                        , destroy: "destroy"
                        , update: "update"
                    }
                }

                , parameterMap: function (option, type) {
                    if (type === "create" || type === "update" ) {
                        //option.GUID = kendo.guid();
                        var option2 = JSON.parse(JSON.stringify(option));
                        option2.Advertisers = "";
                        return option2;
                    } else if (type === "destroy") {
                        return option
                    }
                    return option;
                }
            }//end transport
            , schema: {
                model: {
                    id: "UserID"
                    , fields: {
                        UserID: { type: "string" }
                        , Advertisers: { type: "string" }
                        , Instances: { type: "string" }
                    }
                },
                parse: function (data) {
                    return data;
                }
            }
            , serverPaging: false
            , serverSorting: false
            , serverFiltering: false
            , pageSize: 15
        });
    }

    function RenderInstances(userID) {
        let userMapping;
        let availableCount = new kendo.observable({
            dataSource: undefined,
            displayCount: function () { return "Unmapped Instances (" + this.get("dataSource").total() + ")"; }
        });

        userMapping = new kendo.data.DataSource({
            transport: {
                type: "get",
                read: "GetUserMapping",
                parameterMap: function (option, type) {
                    return { userId: userID, isAdvertiser: false }
                }
            },
            schema: {
                model: {
                    id: "InstanceID"
                    , fields: {
                        InstanceID: { type: "number", from: "MappedID" },
                        InstanceName: { type: "string", from: "MappedName" }
                    }
                }
            }, sort: { field: "InstanceName", dir: "asc" },

        });
        userMapping.read();

        var lyrSelected = $("#selectedInstances").kendoListBox({
            dataTextField: "InstanceName",
            dataValueField: "InstanceID",
            dataSource: userMapping,
            selectable: "multiple"
        }).getKendoListBox();

        userMapping.fetch(function () {
            let excludedID = userMapping.data().map(i => i.InstanceID);
            let filteredDS = instances.data().filter(function (item) {
                return !excludedID.includes(item.InstanceID);
            });

            let dsAvail = kendo.data.DataSource.create({ data: filteredDS, sort: { field: "InstanceName", dir: "asc" } });
            var lyrAvail = $("#optionalInstances").kendoListBox({
                connectWith: "selectedInstances",
                dataTextField: "InstanceName",
                dataValueField: "InstanceID",
                toolbar: {
                    tools: ["transferTo", "transferFrom", "transferAllTo", "transferAllFrom"]
                },
                dataSource: dsAvail,
                selectable: "multiple"
            }).getKendoListBox();

            lyrAvail.wrapper.width(500).height(300);
            lyrSelected.wrapper.width(500).height(300);

            availableCount.set("dataSource", lyrAvail.dataSource);
            kendo.bind($("#availableInstanceCount"), availableCount);
        });


    }

    function RenderAdvertisers(userID) {
        var availableCount = new kendo.observable({
            dataSource: null,
            displayCount: function () {
                if (this.get("dataSource") === null) return "Select a Data Source to populate the list";
                return "Unmapped Advertisers (" + this.get("dataSource").total() + ")";
            }
        });

        var userMapings = new kendo.data.DataSource({
            transport: {
                type: "get",
                read: "GetUserMapping",
                parameterMap: function (option, type) {
                    return { userId: userID, isAdvertiser: true }
                }
            },
            schema: {
                model: {
                    id: "AdvertiserMappingID",
                    fields: {
                        AdvertiserMappingID: { type: "number", from: "MappedID" },
                        AdvertiserNameDisplay: { type: "string", from: "MappedName" }
                    }
                }
            },
            sort: { field: "AdvertiserNameDisplay", dir: "asc" },

        });

        $("#tbTextFilter").val('');
        $("#tbTextFilter").keyup(filterUnmappedAdvertisers);

        var lyrAvail = $("#optionalAdvertisers").kendoListBox({
            connectWith: "selectedAdvertisers",
            dataTextField: "AdvertiserNameDisplay",
            dataValueField: "AdvertiserMappingID",
            toolbar: {
                tools: ["transferTo", "transferFrom", "transferAllTo", "transferAllFrom"]
            },
            dataSource: kendo.data.DataSource.create({ data: [], sort: { field: "AdvertiserNameDisplay", dir: "asc" } }),
            selectable: "multiple"


        }).getKendoListBox();

        var lyrSelected = $("#selectedAdvertisers").kendoListBox({
            dataTextField: "AdvertiserNameDisplay",
            dataValueField: "AdvertiserMappingID",
            dataSource: userMapings,
            selectable: "multiple",
            autoBind: false
        }).getKendoListBox();

        $(document).on("click", "#advertiserMoveRight", function () {
            var listBox = $("#optionalAdvertisers");
            var mappedAdvertisers = getAdvertiserSelectionArray(listBox);
            var listBoxOptional = $("#optionalAdvertisers").data("kendoListBox").dataSource;
            var listBoxSelected = $("#selectedAdvertisers").data("kendoListBox").dataSource;
            updateAdvertiserDatasource(listBoxOptional, listBoxSelected, mappedAdvertisers);
        });

        $(document).on("click", "#advertiserMoveLeft", function () {
            var listBox = $("#selectedAdvertisers");
            var mappedAdvertisers = getAdvertiserSelectionArray(listBox);
            var listBoxOptional = $("#optionalAdvertisers").data("kendoListBox").dataSource;
            var listBoxSelected = $("#selectedAdvertisers").data("kendoListBox").dataSource;
            updateAdvertiserDatasource(listBoxSelected, listBoxOptional, mappedAdvertisers);
        });

        $(document).on("click", "#advertiserMoveAllRight", function () {
            var listBoxOptional = $("#optionalAdvertisers").data("kendoListBox").dataSource;
            var listBoxSelected = $("#selectedAdvertisers").data("kendoListBox").dataSource;
            var mappedAdvertisers = getAdvertiserSelectionArray(null, listBoxOptional);
            updateAdvertiserDatasource(listBoxOptional, listBoxSelected, mappedAdvertisers);
        });

        $(document).on("click", "#advertiserMoveAllLeft", function () {
            var listBoxOptional = $("#optionalAdvertisers").data("kendoListBox").dataSource;
            var listBoxSelected = $("#selectedAdvertisers").data("kendoListBox").dataSource;
            var mappedAdvertisers = getAdvertiserSelectionArray(null, listBoxSelected);
            updateAdvertiserDatasource(listBoxSelected, listBoxOptional, mappedAdvertisers);
        });

        var isEditMode = userID != '';

        $("#tbUserID").kendoDropDownList({
            dataTextField: "UserName",
            dataValueField: "UserName",
            dataSource: isEditMode ? [{ UserName: userID }] : dsRedshiftUsers,
            index: 0,
            filter: "contains",
            dataBound: onUserIDDataBound
        }).getKendoDropDownList();

        if (isEditMode) {
            $("#tbUserID").getKendoDropDownList().select(1);
            $("#tbUserID").getKendoDropDownList().enable(false);
        }

        let selectedDataSource = $("#selectedDataSource").kendoDropDownList({
            dataTextField: "DataSourceName",
            dataValueField: "DataSourceID",
            dataSource: dataSources,
            index: 0,
            value: 0,
            change: onDataSourceChange,
            filter: "contains",
            optionLabel: "Select a DataSource...",
            autoBind: false
        }).getKendoDropDownList();

        kendo.bind($("#availableAdvertiserCount"), availableCount);

        selectedDataSource.select(0);

        lyrAvail.wrapper.width(500).height(300);
        lyrSelected.wrapper.width(500).height(300);

        advertisers.fetch().then(function () {
            displayAdvertiserList(lyrAvail, lyrSelected, advertisers, userMapings, availableCount, 0);
        });

        function onUserIDDataBound() {
            var optionLabel = "Select a User...";
            if ($("#tbUserID").getKendoDropDownList().dataSource.data().length == 0) {
                optionLabel = "No new users available";
            }
            $("#tbUserID").getKendoDropDownList().setOptions({ optionLabel: optionLabel });
        }

        function onDataSourceChange() {
            var value = $("#selectedDataSource").val();
            displayAdvertiserList(lyrAvail, lyrSelected, advertisers, userMapings, availableCount, value);
            resetAdvertiserSearch();
            updateRequiredDataSourceMessageVisibility(value !== "");
        }

        function resetAdvertiserSearch() {
            $("#tbTextFilter").val('');
            filterUnmappedAdvertisers();
        }

        function filterUnmappedAdvertisers() {
            lyrAvail.dataSource.filter({ field: "AdvertiserNameDisplay", operator: "contains", value: $("#tbTextFilter").val() });
        }
    }

    function updateRequiredDataSourceMessageVisibility(hasValueSelected) {
        if (hasValueSelected) {
            $("#requiredDataSourceMessage").hide();
        } else {
            $("#requiredDataSourceMessage").show();
        }
    }

    function displayAdvertiserList(lyrAvail, lyrSelected, advertisers, usermappings, availableCount, dataSourceId) {
        if (dataSourceId === undefined || dataSourceId == '') {
            //the datasource is set to its default, no advertiser should be displayed
            lyrAvail.dataSource.data([]);
            lyrSelected.dataSource.filter({ field: "DataSourceID", operator: "eq", value: dataSourceId });
            availableCount.set("dataSource", null);
            return;
        }

        let excludedID = usermappings.data().map(i => i.AdvertiserMappingID);
        let filteredDS = advertisers.data().filter(function (item) {
            return !excludedID.includes(item.AdvertiserMappingID);
        });
        filteredDS = $.grep(filteredDS, function (e) { return e.DataSourceID == dataSourceId });
        lyrAvail.dataSource.data(filteredDS);

        lyrSelected.dataSource.filter({ field: "DataSourceID", operator: "eq", value: dataSourceId });

        availableCount.set("dataSource", lyrAvail.dataSource);

    }

    function SetValidation(container) {
        var validator = container.find("#lyrEdit").kendoValidator({
            rules: {
                required: function (input) {
                    if (input.is("[class*='required']")) {
                        if (!$.trim(input.val()))
                            return false;
                        return true;
                    }
                    return true;
                },
            }//end rules
            , messages: {
                required: "* required",
            }
            , validateOnBlur: false
        });
    }
    userGrid.thead.on("click", "#btnAdd", function (e) {

        userGrid.addRow();
    });

    function updateAdvertiserDatasource(origin, destination, selection) {

        var originListBox = origin.data();
        var destinationListBox = destination.data();

        var originListBoxArray = Array.from(originListBox);
        var destinationListBoxArray = Array.from(destinationListBox);

        var mappedSelectionArray = Array.from(selection);
        destinationListBoxArray = destinationListBoxArray.concat(mappedSelectionArray);
        originListBoxArray = originListBoxArray.filter(function(el) {
            return !mappedSelectionArray.includes(el);
        });

        origin.data(originListBoxArray);
        destination.data(destinationListBoxArray);
    }

    function getAdvertiserSelectionArray(listbox, dataSource) {
        var mappedAdvertisers = {};
        if (dataSource == null) {
            var selectedAdvertisers = listbox.getKendoListBox().select();
            var listBoxData = listbox.data("kendoListBox");

            mappedAdvertisers = selectedAdvertisers.map(function(val) {
                var dataItem = listBoxData.dataItem(this);
                return dataItem;
            });
        } else {
            var mappedAdvertisersViewData = dataSource.view();
            mappedAdvertisers = Array.from(mappedAdvertisersViewData);
        }

        return mappedAdvertisers;
    }


});