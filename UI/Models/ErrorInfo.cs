namespace Greenhouse.UI.Models
{
    public class ErrorInfo
    {
        public string ExceptionGUID { get; set; }
        public string Host { get; set; }
        public string ActionName { get; private set; }
        public string ControllerName { get; private set; }
        public Exception Exception { get; private set; }

        public ErrorInfo()
        {
        }

        public ErrorInfo(Exception exception, string controllerName, string actionName)
        {
            ArgumentNullException.ThrowIfNull(exception);
            if (string.IsNullOrEmpty(controllerName))
                throw new ArgumentNullException(nameof(controllerName), "Controller is null or empty");
            if (string.IsNullOrEmpty(actionName))
                throw new ArgumentNullException(nameof(actionName), "Controller action is null or empty");
            this.Exception = exception;
            this.ControllerName = controllerName;
            this.ActionName = actionName;
        }
    }
}