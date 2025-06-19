namespace Greenhouse.UI.Models
{
    public class SystemMessage
    {
        public string Title;
        public string Message;
    }

    public class ErrorMessage : SystemMessage
    {
        public int ErrorCode;
    }

    public class ValidationMessages : SystemMessage
    {
        public IEnumerable<string> Messages;
    }

    //A simple message wrapper for ajax POST requests indicating operation status
    public class AjaxResponseMessage : SystemMessage
    {
        public string Id;
        public bool Success;
    }
}