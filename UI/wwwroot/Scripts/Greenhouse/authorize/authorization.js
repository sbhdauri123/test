$(document).ready(function () {
    var url = "/authorizationHub";

    var authorizationHub = new signalR.HubConnectionBuilder()
        .withUrl(url, {
            transport: signalR.HttpTransportType.LongPolling
        })
        .build();
    let lyrKGrid = $("#kGrid");
    let tplcheck = kendo.template($("#tplCheck").html());

    fnTemplateCheck = function (value) {
        var isChecked = (value != '0') || (value === true);
        var htm = tplcheck({
            Val: isChecked
        });
        return htm;
    };

    let kgrid = lyrKGrid.kendoGrid({
        dataSource: AuthorizationDataSource()
        , columns: [
            {
                command: [
                    { name: "edit", text: "", template: "<a class='btn-small lnkEdit'><i class='fa fa-pencil-square-o grid-edit-link'></i></a>", width: 10 }
                    , { name: "destroy", template: "<a class='btn-small lnkDelete'><i class='fa fa-times grid-edit-link'></i></a>", text: "", width: 10 }
                ]
                , width: 120
                , title: "<button id='btnAdd' class='btn btn-primary'>Add User</button>", menu: false
            }
            , { field: "Email", title: "Email", width: 200 }
            , { field: "SAMAccountName", title: "Lion Login", width: 180 }
            , { title: "Is Admin", template: '#=fnTemplateCheck(IsAdmin)#', width: 80 }
            , { field: "CreatedDate", title: "Created Date", template: "${kendo.toString(CreatedDate, 'G')}", width: 180 }
            , { field: "LastUpdated", title: "Last Updated", template: "${kendo.toString(LastUpdated, 'G')}", width: 180 }
        ]
        , dataBound: OnGridDataBind
        , editable: {
            mode: "popup"
            , edit: true
            , destroy: true
            , create: true
            , confirmation: "Are you sure you want to delete this?"
            , template: $("#tplAuthorization").html()

        }
        , resizable: true
        , sortable: true
        , edit: function (evt) {
            var container = evt.container;
            container.width("470").height("470");

            var win = container.data("kendoWindow");
            win.center();
            var title = (evt.model.SAMAccountName == "") ? "Add New User " : "Editing [" + evt.model.SAMAccountName + "]";
            win.title(title);

            SetValidationIntegration(container);

        }, save: function (evt) {
            var validator = evt.container.find("#lyrEditAuthorization").data("kendoValidator");
            if (!validator.validate()) {
                evt.preventDefault();
            }
        }


    }).data("kendoGrid");


    function AuthorizationDataSource() {
        return new kendo.data.DataSource({
            type: "signalr",
            autoSync: false,
            transport: {
                signalr: {
                    promise: authorizationHub.start(),
                    hub: authorizationHub,
                    server: {
                        read: "read"
                        , create: "create"
                        , destroy: "destroy"
                        , update: "update"
                    },
                    client: {
                        read: "read"
                        , create: "create"
                        , destroy: "destroy"
                        , update: "update"
                    }
                }
            }//end transport
            , schema: {
                model: {
                    id: "UserAuthorizationID"
                    , fields: {
                        UserAuthorizationID: { type: "number" }
                        , Email: { type: "string" }
                        , SAMAccountName: { type: "string" }
                        , IsAdmin: { type: "boolean", default: false }
                        , CreatedDate: { type: "date", format: "MM/dd/yyyy", parse: function (value) { return parseUTCDate(value) } }
                        , LastUpdated: { type: "date", format: "MM/dd/yyyy", parse: function (value) { return parseUTCDate(value) } }
                    }
                }
                , total: function (data) { return data.length; }
            }
            , requestEnd: function (e) {
                kendo.ui.progress(lyrKGrid, false);
            }
            , requestStart: function (e) {
                kendo.ui.progress(lyrKGrid, true);
            }
            , serverPaging: false
            , serverSorting: false
            , serverFiltering: false
            , pageSize: 20
            , error: function (e) {
                if (e.xhr.toString().indexOf("Error: The DELETE") != -1) {
                    //http://www.telerik.com/forums/rows-disappeared-from-grid-on-delete-fail
                    kgrid.cancelChanges();
                    alert("This record can not be deleted because there are associated elements. Please delete all associated elements first.");
                }
            }
        });//end dataSource
    }//end DataSource()



    function SetValidationIntegration(container) {
        var validator = container.find("#lyrEditAuthorization").kendoValidator({
            rules: {
                required: function (input) {
                    if (input.is("[class*='required']")) {
                        if (!$.trim(input.val()))
                            return false;
                        return true;
                    }
                    return true;
                },
                uniqueValues: function (input) {
                    if (!input.hasClass("unique"))
                        return true;

                    let inputs = container.find(".unique");
                    let grid = lyrKGrid.getKendoGrid();
                    let arr = []
                    inputs.each(function (i, a) {
                        var str = { field: $(a).attr("id"), operator: "eq", value: $(a).val().toString() }
                        arr.push(str);
                    });

                    let filteredSet = kendo.data.Query.process(grid.dataSource.data(), { filter: arr });
                    return !(filteredSet.data.length > 1);
                }

            }//end rules
            , messages: {
                required: "* required",
                uniqueValues: "* must be unique"
            }
            , validateOnBlur: false
        });
    }
});