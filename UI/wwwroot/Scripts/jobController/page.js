
window.onload =$(function () {

	var gridDataSource = DataLoadDataSource();

	$("#kGrid").kendoGrid({
		dataSource: gridDataSource
		, columns: [
			{
				command: [{ name: "edit", text: "", template: "<a class='btn-small lnkEdit'><i class='fa fa-pencil-square-o grid-edit-link'></i></a>", width: 10 }
					, { name: "destroy", template: "<a class='btn-small lnkDelete'><i class='fa fa-times grid-edit-link'></i></a>", text: "", width: 10 }], width: 120
				, title: "<button id='btnAdd' class='btn btn-primary'>Create Job</button>"
			}
			, { field: "Description", title: "Job Type", width: "200px" }
			, { field: "JobGroup", title: "Source", width: "180px" }
			, { field: "Interval", title: "Interval", width: "150px" }
			, { field: "NextFireTimeStr", title: "Next Fire Time", width: "200px" }
			, { hidden: true, field: "NextFireTime", title: "Next Fire Time", width: "200px" }
			, { field: "PrevFireTimeStr", title: "Prev Fire Time", width: "200px" }
		]
		, editable: {
			mode: "popup"
			, edit: true
			, destroy: true
			, create: true
			, confirmation: "Are you sure you want to delete this?"
			, template: $("#tplJobSchedule").html(),
			template: $("#template").html()
		}
		, dataBound: function (evt) {
			var kgrid = this;
			BindGridButtons(kgrid, evt);
		}
		, edit: function (evt) {

			var container = evt.container;

			if (evt.model.isNew()) {
				container.width("550").height("550");

				var win = container.data("kendoWindow");
				win.center();
				var title = "Schedule New Job";
				win.title(title);

				var t = container.find("#Time").kendoDateTimePicker({
					value: new Date(),
					format: "yyyy/MM/dd hh:mm tt"
				}).data("kendoDateTimePicker");

				container.find("#JobSchedulerTypeBackfillDateFrom").kendoDatePicker({
					value: new Date(),
					format: "yyyy/MM/dd"
				});
				container.find("#JobSchedulerTypeBackfillDateTo").kendoDatePicker({
					value: new Date(),
					format: "yyyy/MM/dd"
				});
				container.find("#ApiEntities").kendoMultiSelect({
					dataTextField: "text",
					dataValueField: "value",
					autoClose: false,
					tagMode: "single"
				});


				if (evt.model.JobName == "") {
					t.value(new Date());
					var currentDate = new Date();
					t.min(new Date(currentDate.getFullYear(), currentDate.getMonth(), currentDate.getDate()))
				}

				container.find(".k-grid-update").html("<span class='k-icon k-update'></span>Schedule");

				var intervalDdl = $(container).find("#Interval").kendoDropDownList({
					dataSource: intervals
					, dataTextField: "Name"
					, dataValueField: "ID"
					, optionLabel: "--Select Interval--"
					, dataBound: function (e) {
						var text = evt.model.Interval;
						if (evt.model.Interval.indexOf("Monthly") > -1) {
							text = "Monthly";
						} else if (evt.model.Interval.indexOf(",") > -1) {
							text = "Weekly";
							$(container).find("#daysection").removeClass("hide");
						}

						if (text == "Hourly") $(container).find("#hoursection").removeClass("hide");
						else if (text == "Minutely") $(container).find("#minutesection").removeClass("hide");

						//intervalDdl.select(function (dataItem) {
						//    return dataItem.ID == text;
						//});
					}
					, change: function () {

						var val = this.dataItem().Name;
						var d = $(container).find("#daysection");
						var h = $(container).find("#hoursection");
						var m = $(container).find("#minutesection");
						$(d).addClass("hide");
						$(h).addClass("hide");
						$(m).addClass("hide");

						if (val == "Weekly") $(d).removeClass("hide");
						else if (val == "Hourly") $(h).removeClass("hide");
						else if (val == "Minutely") $(m).removeClass("hide");

					}
				}).data("kendoDropDownList");


				$(container).find('div#jobTypeTabs ul a').click(function (e) {
					//var target = e.target.text.trim(); //.replace(/\s+/g, '');
					var jobTypeId = $(this).attr("data-id");
					var jobStepId = $(this).attr("data-stepid");

					$('#jobTabs li.active').removeClass('active');
					$(this).parent().first('li').addClass('active');
					 
					$("#jobschedulertypesection").addClass("hide");
					$("#jobschedulertypebackfilldatesection").addClass("hide");
					$("#IntervalSection").removeClass("hide");
					


					if (jobStepId) {
						$("#selectedSourceJobStepID").val(jobStepId);
						$("#jobSection").addClass("hide");
						$("#sourceSection").removeClass("hide");
						PopulateSources(jobStepId);

						GetSourceJobStep(jobStepId);

						$("#jobschedulertypesection").removeClass("hide");
						$('input[name=JobSchedulerType]#Recurring').prop('checked', 'checked');
					}
					else {
						$('input[name=JobSchedulerType]').prop('checked', false);
						$("#jobSection").removeClass("hide");
						PopulateJobs(jobTypeId);

						var ddlGreenhouseJob = $("#GreenhouseJob").data("kendoDropDownList");
						ddlGreenhouseJob.select(0);
						ddlGreenhouseJob.trigger("change");

						e.preventDefault();
						var selectedScheduler = $(e.target).parents('li.dropdown').children('a.jobType').text().trim() + " > " + e.target.text.trim();
						$('div#selectedScheduler p').text(selectedScheduler);
					}

				});

				$(container).find("input[type='radio'][name='JobSchedulerType']").click(function (e) {
					var id = $(this).attr("id");
					if (id == "Backfill") {
						$("#jobschedulertypebackfilldatesection").removeClass("hide");
						$("#IntervalSection").addClass("hide");
					}
					else {
						$("#jobschedulertypebackfilldatesection").addClass("hide");
						$("#IntervalSection").removeClass("hide");
					}
				});

				$(container).find("input[type='checkbox'][name='BackfillDimensionOnly']").click(function (e) {
					if ($(this).is(':checked')) {
						$("#backfillDatesSection").addClass("hide");
					}
					else {
						$("#backfillDatesSection").removeClass("hide");
					}
				});


				Initialize(container);

			}
			else {
				onEdit(evt);

				container.width("450").height("375");

				var win = container.data("kendoWindow");
				var title = "Editing [" + evt.model.Description + " - " + evt.model.JobGroup + "]";
				console.log("title", title);
				win.title(title);

				var nextfireTime = new Date(evt.model.NextFireTimeString);

				var t = container.find("#TimeEdit").kendoDateTimePicker({
					value: new Date(),
					format: "yyyy/MM/dd hh:mm tt"
				}).data("kendoDateTimePicker");

				t.value(nextfireTime);

				var currentDate = new Date();
				currentDate.setMinutes(currentDate.getMinutes() + 1);   // add 1 minute to the current time

				t.min(new Date(currentDate.getFullYear(), currentDate.getMonth(), currentDate.getDate(), currentDate.getHours(), currentDate.getMinutes(), 0, 0));

				// Below is an explanation of the Kendodatetimepicker's minimum setting and interval relationship.
				// https://stackoverflow.com/questions/39737746/kendo-datetimepicker-set-min-value-will-change-the-time-options

				win.center().open();
			}
			SetValidation(container);
		}
		, save: function (evt) {
			var container = evt.container;

			var validator = evt.container.find(".lyrEdit").data("kendoValidator");
			if (!validator.validate()) {
				evt.preventDefault();
				return;
			}

			if (evt.model.isNew()) {
				// ====================================================================

				// CHECK TO SEE IF THE JOB EXISTS ALREADY IN THE GRID... 

				var description = $(container).find("#GreenhouseJob").data("kendoDropDownList").text();

				if ($(container).find("#greenhouseSourceId").data("kendoDropDownList") != null) {
					var jobGroup = $(container).find("#greenhouseSourceId").data("kendoDropDownList").text();

					gridLength = this.dataSource.data().length;

					var JobExists = false;
					for (var i = 0; i < gridLength; i++) {
						if (this.dataSource.data()[i].Description == description && this.dataSource.data()[i].JobGroup == jobGroup && this.dataSource.data()[i].Interval != "Backfill") {
							JobExists = true;
							break;
						}
					}

					if (JobExists == true && !IsAggregateBackfillSelected()) {
						showMessage("The combination of [" + description + "] - [" + jobGroup + "] exists already.")

						evt.preventDefault();
						return;
					}
				}
				else {
					gridLength = this.dataSource.data().length;

					var JobExists = false;
					for (var i = 0; i < gridLength; i++) {
						if (this.dataSource.data()[i].Description == description) {
							JobExists = true;
							break;
						}
					}

					if (JobExists == true) {
						showMessage("[" + description + "] exists already.")

						evt.preventDefault();
						return;
					}
				}

				// ====================================================================


				var allVals = [];
				var b = $(container).find("#timesection");
				$('#daysection :checked').each(function () {
					allVals.push($(this).val());
				});

				var timepicker = evt.container.find("#Time");
				var datestring = kendo.toString(timepicker.val(), "yyyy-MM-dd h:mm:ss tt");
				evt.model.Time = datestring;

				var interval = evt.container.find("#Interval").data("kendoDropDownList");
				evt.model.Interval = IsAggregateBackfillSelected() ? "Backfill" : interval.text();
				evt.model.Days = allVals.join(",");
				evt.model.JobTypeId = $(container).find('div#jobTypeTabs ul a').attr("data-id"); //$(container).find("#GreenhouseJob").val();
				evt.model.SourceId = $(container).find("#greenhouseSourceId").val();
				evt.model.SourceJobStepID = $(container).find("#selectedSourceJobStepID").val();

				evt.model.AggregateIsBackfill = IsAggregateBackfillSelected();
				evt.model.AggregateBackfillDateFrom = $(container).find("#JobSchedulerTypeBackfillDateFrom").val();
				evt.model.AggregateBackfillDateTo = $(container).find("#JobSchedulerTypeBackfillDateTo").val();
				evt.model.AggregateApiEntities = $("#ApiEntities").data("kendoMultiSelect").value();

				evt.model.BackfillDimOnly = document.querySelector("#BackfillDimensionOnly").checked;

			}
			else {
				var timepicker = evt.container.find("#TimeEdit");

				var datestring = kendo.toString(timepicker.val(), "yyyy-MM-dd h:mm:ss tt");

				evt.model.Time = datestring;

				evt.model.dirty = true;
			}
			kendo.ui.progress($(container), true);

		}
		, resizable: true
		, pageable: true
		, scrollable: true
		, sortable: {
			mode: "single",
			allowUnsort: true
		}
	}).data("kendoGrid")
		.wrapper.height("650px");
	gridDataSource.bind("error", OnDataSourceError);

	// ========================================================
	// Used to make AdvertiserName non-editable
	function onEdit(e) {
		$("#Description").attr("readonly", true);
		$("#JobGroup").attr("readonly", true);
	}

	// ========================================================
	//THE FOLLOWING REFRESHES THE GRID UPON SAVE, BY RE-READING
	var grid = $("#kGrid").data("kendoGrid");
	grid.bind("save", onGridSave);

	function onGridSave(e) {
		e.sender.one("dataBound", function () {
			e.sender.dataSource.read();
		});
	};
	// ========================================================


	function PopulateJobs(jobTypeId) {

		var data = [];
		$("#jobSection").removeClass("hide");
		$("#sourceSection").addClass("hide");

		$.ajax({
			url: "GetJobTypes",
			data: { jobTypeId: jobTypeId },
			success: OnGetJobTypeSuccess
		});

	}

	function OnGetJobTypeSuccess(data) {
		console.log(data);
		var ddlGreenhouseJob = $("#GreenhouseJob");
		ddlGreenhouseJob.kendoDropDownList({
			dataSource: data
			, dataTextField: "ShortDescription"
			, dataValueField: "SourceJobStepID"
			, optionLabel: { SourceJobStepID: "-1", ShortDescription: "--Select Job Type--" }
			, change: function (e) {

				if (this.dataItem(e.item).IsBatch == false) {
					$("#sourceSection").removeClass("hide");

					var val = this.value();
					PopulateSources(val);
				}
				else {
					$("#sourceSection").addClass("hide");
				}

				$("#selectedSourceJobStepID").val(this.value());
			}
		});
	}

	function PopulateSources(SourceJobStepID) {
		var data = [];
		$.ajax({
			url: "GetSources",
			data: { SourceJobStepID: SourceJobStepID },
			success: OnGetSourceSuccess
		});
	}

	function GetSourceJobStep(sourceJobStepID) {
		var data = [];
		$.ajax({
			url: "GetSourceJobStep",
			data: { sourceJobStepID: sourceJobStepID },
			success: OnGetSourceJobStepSuccess
		});
	}

	function OnGetSourceSuccess(data) {
		var ddlGreenhouseSourceId = $("#greenhouseSourceId");
		ddlGreenhouseSourceId.kendoDropDownList({
			dataSource: data
			, dataTextField: "SourceName"
			, dataValueField: "SourceID"
			, optionLabel: { SourceID: "-1", SourceName: "--Select Source--" }
			, change: OnSourceChange
		});
	}

	function OnGetSourceJobStepSuccess(data) {
		$("#GreenhouseJob").kendoDropDownList({
			dataSource: [data.ShortDescription]
		});
		$("#GreenhouseJob").data('kendoDropDownList').value(data.ShortDescription);
	}

	function OnSourceChange(data) {

		//Reset any UI changes associated with 
		resetBackfillSection();

		if (data.sender.selectedIndex == 0) {
			$("#ApiEntities").data("kendoMultiSelect").dataSource.data([]);
			return;
		}

		var aggregateInitializeSettings = data.sender.options.dataSource[data.sender.selectedIndex - 1].AggregateInitializeSettings;
		if (aggregateInitializeSettings) {
			//Wrapping this logic in a try catch because some of the test AggregateInitializeSettings do not deserialize and can throw an exception
			try {
				var jsonString = JSON.stringify(eval("(" + aggregateInitializeSettings + ")"));
				var deserializedSettings = JSON.parse(jsonString);
				if (deserializedSettings?.CanSelectBackfillOptions) {
					$("#backfillDimensionOnlySection").removeClass("hide");
				}
			}
			catch {
			}
		}

		var sourceId = data.sender.options.dataSource[data.sender.selectedIndex - 1].SourceID;
		$.ajax({
			url: "GetAPIEntityIds",
			data: { sourceId: sourceId },
			success: OnGetAPIEntityIdsSuccess
		});
	}

	function OnGetAPIEntityIdsSuccess(data) {
		var newEntities = data.map(function(d) {
			return {
				text: d.APIEntityCode + (d.APIEntityName.length > 0 ? ' (' + d.APIEntityName + ')' : ''),
				value: d.APIEntityID
			}
		});
		$("#ApiEntities").data("kendoMultiSelect").dataSource.data(newEntities);
	}

	function DataLoadDataSource() {
		return new kendo.data.DataSource({
			transport: {
				read: {
					url: "GetScheduledJobs"
					, type: "GET"
					, dataType: "json"
					, complete: function (jqXHR, textStatus) { kendo.ui.progress($('#kGrid'), false); }
				}
				, create: {
					url: "ScheduleJob"
					, type: "POST"
					, dataType: "json"
					, complete: function (jqXHR, textStatus) { kendo.ui.progress($('#kGrid'), false); }
				}
				, update: {
					url: "UpdateJobNextFireTime"
					, type: "POST"
					, dataType: "json"
					, complete: function (jqXHR, textStatus) { kendo.ui.progress($('#kGrid'), false); }
				}
				, destroy: {
					url: "DeleteJob"
					, type: "POST"
					, dataType: "json"
					, complete: function (jqXHR, textStatus) {
						kendo.ui.progress($('#kGrid'), false);
					}
				}
				, parameterMap: function (option, type) {
					return option;
				}
			}
			, schema: {
				model: {
					id: "JobName"
					, fields: {
						JobName: { type: "string" }
						, Description: { type: "string" }
						, JobGroup: { type: "string" }
						, TriggerGroup: { type: "string" }
						, TriggerName: { type: "string" }
						, TriggerState: { type: "string" }
						, NextFireTimeStr: { type: "string" }
						, PrevFireTimeStr: { type: "string" }
						//, Time: { type: "date", format: "yyyy/MM/dd hh:mm tt", defaultValue: new Date() }
						, Time: { type: "date", format: "yyyy-MM-dd HH:mm:ss.sss", defaultValue: new Date() }
						, Interval: { type: "string" }
						, Days: { type: "string" }
						, Hours: { type: "string" }
						, Minutes: { type: "string" }
						, SourceJobID: { type: "string" }
						, AggregateApiEntities: { type: "string" }
						, AggregateIsBackfill: { type: "string" }
						, AggregateBackfillDateFrom: { type: "string" }
						, AggregateBackfillDateTo: { type: "string"}

					}
				}
				, total: function (data) { return data.length; }
			}
			, requestEnd: function (e) {
				kendo.ui.progress($(".k-grid"), false);
			}
			, requestStart: function (e) {
				//kendo.ui.progress($(".k-grid:visible"), true);
			}
			, serverPaging: false
			, serverSorting: false
			, serverFiltering: false
			, pageSize: 50
		});//end dataSource
	}//end DataSource()

	function IsAggregateBackfillSelected() {
		if ($("input[type='radio'][name='JobSchedulerType']").filter(":visible").length == 0) return false;
		return ($("input[type='radio'][name='JobSchedulerType']:checked")[0].id == "Backfill");
	}

	function SetValidation(container) {

		var validator = container.find(".lyrEdit").kendoValidator({
			rules: {
				hourly: function (input) {
					if ($(input).attr('id') == "txtHours") {
						if (container.find("#Interval").val() == intervals.find(function (obj) {
							return obj.Name === "Hourly";
						}).ID) {
							if (!$.trim(input.val())) { return false; }
						}
					}
					return true;
				},
				minutely: function (input) {
					if ($(input).attr('id') == "txtMinutes") {
						if (container.find("#Interval").val() == intervals.find(function (obj) {
							return obj.Name === "Minutely";
						}).ID) {
							if (!$.trim(input.val())) { return false; }
						}
					}
					return true;
				},

				weekly: function (input) {
					if ($(input).attr('id') == "Interval") {
						if ($(input).val() == intervals.find(function (obj) {
							return obj.Name === "Weekly";
						}).ID) {
							var returnValue = false;
							var d = $(container.find("#daysection"));
							$(d).children("input:checked").each(function () {
								if (this.checked) {
									returnValue = true;
								}
							});
							return returnValue;
						}
					}
					return true;
				},

				customrequired: function (input) {
					//alert(($(input).attr('id')));
					if (input.is("[class='required']")) {
						if (!$.trim(input.val())) return false;
					}
					if (container.find(".btn:first").hasClass("active")) {
						if ($(input).attr('id') == "greenhouseSourceId") {
							if (!$.trim(input.val())) return false;
						}
					}
					return true;
				},
				datepicker: function (input) {
					var currentDate = new Date();
					var dtpValue;
					if (input.is("[data-role=datetimepicker]")) {
						dtpValue = input.data("kendoDateTimePicker").value();
						if (dtpValue === null) { return false; }
						if (dtpValue < currentDate) { return false; }
					}
					return true;
				},
				backfilldatepicker: function (input) {
					var currentDate = new Date();
					var dtpValue;
					if (input.is("#JobSchedulerTypeBackfillDateFrom") || input.is("#JobSchedulerTypeBackfillDateTo")) {
						if ($("#JobSchedulerTypeBackfillDateFrom").filter(":visible").length == 0) return true;
						if (!IsAggregateBackfillSelected()) return true;

						dtpValue = input.data("kendoDatePicker").value();
						if (dtpValue === null) { return false; }
						if (dtpValue >= currentDate) { return false; }						
					}
					return true;
				},
				backfillapientity: function(input) {
					
					if (input.is("#ApiEntities")) {
						if ($("#ApiEntities").filter(":visible").length == 0) return true;
						if (!IsAggregateBackfillSelected()) return true;

						var apiEntities = input.data("kendoMultiSelect").value();
						if (apiEntities.length == 0) {
							return false;
						}
					}
					return true;
				},
				jobtype: function (input) {
					if ($(input).attr('id') == "GreenhouseJob") {
						if (container.find("#GreenhouseJob").val() < 0)  return false;
					}
					return true;
				},
				intervalType: function (input) {
					if ($(input).attr('id') == "Interval") {
						if (container.find("#Interval").val() == "" && !IsAggregateBackfillSelected()) return false;
					}
					return true;
				},
			}//end rules
			, messages: {
				hourly: "* hours required",
				minutely: "* minutes required",
				weekly: "* days required",
				customrequired: "* required",
				backfilldatepicker: "Enter a valid past date!",
				backfillapientity: "Select at least one API Entity ID",
				datepicker: "Enter a valid future date!",
				jobtype: "* job type required",
				intervalType: "* interval type required"

			}
			, validateOnBlur: false
		});
	}

	function OnDataSourceError(e) {
		if (!$("#dialog").data("kendoWindow")) {
			$('#dialog').kendoWindow({
				title: "Error",
				modal: true,
				height: 200,
				width: 500
			});
		}
		var dialog = $("#dialog").data("kendoWindow");
		dialog.content(e.xhr.responseText);
		dialog.center();
		dialog.open();
		$("#kGrid").data("kendoGrid").cancelChanges();
	}

	function Initialize(container) {
		var jobTypeId = jobTypes[0].ID; //.replace(/\s+/g, '');
		PopulateJobs(jobTypeId);
	}

	function showMessage(message) {
		if (message.length > 0) {
			$("#stallion-message").show();
			$("#messageText").text(message);
			//setTimeout(function() {
			//    $("#stallion-message").hide();
			//}, 50000);
		}
	}
});


function renderDetailsTemplate(data) {
	return kendo.Template.compile($('#tplJobSchedule-Details').html())(data);
}

function resetBackfillSection() {
	$("#backfillDimensionOnlySection").addClass("hide");
	$("#backfillDatesSection").removeClass("hide");
	$('#BackfillDimensionOnly').prop('checked', false); 
}
