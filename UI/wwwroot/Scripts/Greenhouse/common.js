var parser = document.createElement('a');
parser.href = document.location;
var baseUrl = "//" + parser.hostname + ":" + parser.port +  parser.pathname.substr(0, parser.pathname.lastIndexOf("/"));
//var baseUrl = "//" + parser.hostname + ":" + parser.port + '/' + parser.pathname.substr(0, parser.pathname.lastIndexOf("/"));
//alert(baseUrl);
var secret = "716582686978907365";
var input = "";
var timer;
var mode = false;
var fnRenderPopoverColumn;
var fnRenderCustomPopovers;

var gridColumMenuOptions = {
    sortable: false,
    messages: {
        columns: "Select Column(s)",
        filter: "Filter",
        settings: "Column Options"

    }
};
var gridFilterOptions = {
    extra: false,
    operators: {
        string: {
            startswith: "Starts with",
            eq: " Equals to",
            neq: " Not Equal to ",
            contains: "Contains"
        }
    },
    messages: {
        info: "Filter by",
        selectValue: "--Select--"
    }
};


var escapeChars = {
	"\b": "\\b",
	"\t": "\\t",
	"\n": "\\n",
	"\f": "\\f",
	"\r": "\\r",
	"\"": '\\"',
	"\\": "\\\\"
};


function LookupDataSource(lookupKey) {
    var ds = new kendo.data.DataSource({
        transport: {
            read: {
                url: baseUrl + "/GetLookupValuesByKey"
                    , type: "GET"
                    , dataType: "json"
                    , data: { key: lookupKey }
            }
        },
        sort: { field: "Order", dir: "asc" },
        schema:
        {
            parse: function (data) {
                if (data && data.length > 0)
                    return data[0].LookupValues.map(function (obj, i) {
                        arrObj = obj.split(':')
                        return { ID: arrObj[0], Name: arrObj[0], Order: (arrObj.length > 1) ? arrObj[1] : 1 };
                    });
            }
        }
    });


    return ds;

}

function LookupDataSourceCommaDelimit(lookupKey) {
    var ds = new kendo.data.DataSource({
        transport: {
            read: {
                url: baseUrl + "/GetLookupValuesByKey"
                    , type: "GET"
                    , dataType: "json"
                    , data: { key: lookupKey }
            }
        },
        sort: { field: "Order", dir: "asc" },
        schema:
        {
            parse: function (data) {
                if (data && data.length > 0) {

                    var lookup = data[0].LookupValues;
                    var lookupArr = lookup.toString().split(',')
                    var arr = [];
                    for (index = 0; index < lookupArr.length; ++index) {
                        var o = {};
                        o.ID = lookupArr[index];
                        o.Name = lookupArr[index];
                        arr.push(o);
                    }
                    return arr;
                }
            }
        }
    });


    return ds;

}
function GetDataSource(action, fieldID, fieldName) {
    return new kendo.data.DataSource({
        transport: {
            read: {
                url: baseUrl + "/" + action
                , type: "GET"
                , dataType: "json"
            }
        }
        , schema: {
            parse: function (data) {
                return data.map(function (obj, i) {
                    return { ...obj, ID: obj[fieldID], Name: obj[fieldName] };
                });
            }
        }//end schema
    });
}

function GetDataSourceFullMapping(action, mappingKeys) {
	return new kendo.data.DataSource({
		transport: {
			read: {
				url: baseUrl + "/" + action
				, type: "GET"
				, dataType: "json"
			}
		}
		, schema: {
			parse: function (data) {
				return data.map(function (obj, i) {
					var o = {};
					for(var key in mappingKeys) {
						if (mappingKeys.hasOwnProperty(key)) {
							var objectKeyName = mappingKeys[key];
							o[key] = obj[objectKeyName];
						}
					};
					return o;
				});
			}
		}//end schema
	});
}



function OnGridDataBind(evt)
{
    var kgrid = this;
    BindGridButtons.apply(this, [kgrid, evt]);
    $("div").find(".popovers").popover({ placement: "bottom", trigger: "hover", html: true });
    $("div").find(".popoversList").popover({ placement: "bottom", trigger: "hover", html: true });
}

function fnShowLoading(toggle) {
    kendo.ui.progress($("#mainBody"), toggle)
}

function ValidUrl(str) {
    var pattern = new RegExp('^((https?|ftp|gcs):\\/\\/)?' + // protocol
    '((([a-z\\d]([a-z\\d-]*[a-z\\d])*)\\.)+[a-z]{2,}|' + // domain name
    '((\\d{1,3}\\.){3}\\d{1,3}))' + // OR ip (v4) address
    '(\\:\\d+)?(\\/[-a-z\\d%_.~+]*)*' + // port and path
    '(\\?[;&a-z\\d%_.~+=-]*)?' + // query string
    '(\\#[-a-z\\d_]*)?$', 'i'); // fragment locator
    if (!pattern.test(str)) {
        return false;
    } else {
        return true;
    }
}

function BindGridButtons(kgrid, gridEvt) {
    // show/remove no records message.
    if (kgrid.dataSource.total() == 0) {
        var columnCount = kgrid.thead.find(".k-header").length;
        kgrid.tbody.append('<tr class="empty-grid alert"><td colspan="' + columnCount + '"><div>No records</div></td></tr>');
    }
    else {
        kgrid.tbody.find("tr.empty-grid alert").remove();

    }

    // check if events are already bound to buttons
    if ($._data($(kgrid.thead)[0], 'events') && !$._data($(kgrid.thead)[0], 'events').click) {
        kgrid.thead.on("click", "#btnAdd", gridEvt, function (e) {
            e.data.sender.addRow();
        });
    }

    var editEvent = $._data($(kgrid.tbody)[0], 'events');
    if (!editEvent || !$.grep(editEvent.click, function (a, i) { return a.selector == '.btn-small.lnkEdit' })) {
        kgrid.tbody.on("click", ".btn-small.lnkEdit", gridEvt, function (e) {
            var row = $(this).parent().closest("tr");
            e.data.sender.editRow(row);
        });
    }

    if (!editEvent || !$.grep(editEvent.click, function (a, i) { return a.selector == '.btn-small.lnkDelete' })) {
        kgrid.tbody.on("click", ".btn-small.lnkDelete", gridEvt, function (e) {
            var row = $(this).parent().closest("tr");
            e.data.sender.removeRow(row);
        });
    }
}

function parseUTCDate(value) {
    if (value.endsWith("Z")) {
        // The value represents a UTC date
        return new Date(value);
    } else {
        // Parse the incoming value as UTC date
        return new Date(Date.parse(value + "Z"));
    }
}
