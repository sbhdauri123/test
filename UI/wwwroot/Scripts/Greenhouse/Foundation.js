var namespace, type;

namespace = function(name, separator, container) {
  var ns, o, _i, _len;

  ns = name.split(separator || ".");
  o = container || window;
  for (_i = 0, _len = ns.length; _i < _len; _i++) {
    name = ns[_i];
    o = o[name] = o[name] || {};
  }
  return o;
};

type = (function() {
  var classToType, name, _i, _len, _ref;

  classToType = {};
  _ref = "Boolean Number String Function Array Date RegExp Undefined Null".split(" ");
  for (_i = 0, _len = _ref.length; _i < _len; _i++) {
    name = _ref[_i];
    classToType["[object " + name + "]"] = name.toLowerCase();
  }
  return function(obj) {
    var strType;

    strType = Object.prototype.toString.call(obj);
    return classToType[strType] || "object";
  };
})();

(function() {
  var console, method, methods, noop, _i, _len;

  noop = function() {};
  methods = ['assert', 'clear', 'count', 'debug', 'dir', 'dirxml', 'error', 'exception', 'group', 'groupCollapsed', 'groupEnd', 'info', 'log', 'markTimeline', 'profile', 'profileEnd', 'table', 'time', 'timeEnd', 'timeStamp', 'trace', 'warn'];
  console = (window.console = window.console || {});
  for (_i = 0, _len = methods.length; _i < _len; _i++) {
    method = methods[_i];
    if (console[method] == null) {
      console[method] = noop;
    }
  }
})();
