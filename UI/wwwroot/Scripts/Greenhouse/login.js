namespace("com.greenhouse.Login");

com.greenhouse.Login = (function () {
    var BindAnchors, ResetAnchors, UnbindAnchors, anchors, panels, _anchorContainerElement, _containerElement, _statusElement;

    _containerElement = void 0;

    _anchorContainerElement = void 0;

    _statusElement = void 0;

    panels = {};

    anchors = {};

    ResetAnchors = function () {
        var anchor, currAnchor, id;

        if (_anchorContainerElement != null) {
            $(_anchorContainerElement).empty();
            for (id in anchors) {
                anchor = anchors[id];
                if (_anchorContainerElement.tagName === "ul") {
                    currAnchor = $("<li>").append(anchor);
                } else {
                    currAnchor = anchor;
                }
                $(_anchorContainerElement).append(currAnchor);
            }
        }
    };

    BindAnchors = function () {
        var _this = this;

        if (_anchorContainerElement != null) {
            $(_anchorContainerElement).on("click.login", "a.anchor", function (event) {
                var targetPanel;

                targetPanel = $(event.target).data("panel");
                _this.ActivatePanel(targetPanel);
            });
        }
    };

    UnbindAnchors = function () {
        if (_anchorContainerElement != null) {
            $(_anchorContainerElement).off(".login", "a.anchor");
        }
    };

    Login.prototype.ShowProgressPanel = function (display) {
        if (display == null) {
            display = false;
        }
        if (_containerElement != null) {
            kendo.ui.progress(_containerElement, display);
        }
    };

    Login.prototype.ActivatePanel = function (panelId) {
        var panel, panelName;

        if (panels != null) {
            for (panelName in panels) {
                panel = panels[panelName];
                $(panel).hide();
                if (panelName === panelId) {
                    if (_containerElement != null) {
                        $(_containerElement).attr('class', panelId);
                    }
                    if (anchors != null) {
                        $(anchors[panelName]).hide();
                    }
                    $(panel).fadeIn(250);
                } else {
                    if (anchors != null) {
                        $(anchors[panelName]).show().css('display', 'inline-block');
                    }
                }
            }
        }
        this.ShowMessage("");
    };

    Login.prototype.ShowMessage = function (message, cssClass) {
        if (cssClass == null) {
            cssClass = "alert-error";
        }
        if (_statusElement != null) {
            if (message != null) {
                $(_statusElement).attr('class', cssClass).html(message);
            } else {
                $(_statusElement).attr('class', '').empty();
            }
        }
    };

    Login.ValidateEmail = function (emailAddress) {
        var pattern, x;

        pattern = "^([a-zA-Z0-9_\-\.]+)@@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.)|(([a-zA-Z0-9\-]+\.)+))([a-zA-Z]{2,4}|[0-9]{1,3})(\]?)$";
        x = pattern.test(emailAddress);
        alert(x);
        return x;
    };

    Login.prototype.anchorContainerElement = function (element) {
        if (element != null) {
            UnbindAnchors.apply(this);
            _anchorContainerElement = element;
            BindAnchors.apply(this);
            ResetAnchors();
        }
        return _anchorContainerElement;
    };

    Login.prototype.containerElement = function (element) {
        var defaultPanel;

        if (element != null) {
            defaultPanel = void 0;
            _containerElement = element;
            $(element).find(".loginPanel").each(function (index, element) {
                var anchor, panelId, panelName;

                panelId = $(element).data("panel");
                panelName = element.title;
                if ($(element).data("defaultpanel")) {
                    defaultPanel = panelId;
                }
                anchor = $("<a>").addClass("anchor").data('panel', panelId).text(panelName);
                anchors[panelId] = anchor;
                panels[panelId] = element;
            });
            ResetAnchors();
            if (defaultPanel != null) {
                this.ActivatePanel(defaultPanel);
            }
        }
        return _containerElement;
    };

    Login.prototype.statusElement = function (element) {
        if (element != null) {
            if (_statusElement != null) {
                $(_statusElement).empty();
            }
            _statusElement = element;
        }
        return _statusElement;
    };

    function Login(container, anchorContainer, status) {
        if (container != null) {
            this.containerElement(container);
            if (anchorContainer != null) {
                this.anchorContainerElement(anchorContainer);
            }
            if (status != null) {
                this.statusElement(status);
            }
        }
    }

    return Login;

})();
