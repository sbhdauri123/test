
namespace Greenhouse.DAL.SOAP
{
    using System.ServiceModel.Description;
    using System.ServiceModel.Dispatcher;

    public class CustomInspectorBehavior : IEndpointBehavior
    {
        private CustomMessageInspector inspector;

        public string Request => inspector.Request;
        public string Reply => inspector.Reply;

        public void AddBindingParameters(ServiceEndpoint endpoint, System.ServiceModel.Channels.BindingParameterCollection bindingParameters)
        {
            // No binding parameters need to be modified
        }

        public void ApplyClientBehavior(ServiceEndpoint endpoint, ClientRuntime clientRuntime)
        {
            inspector = new CustomMessageInspector();
            clientRuntime.ClientMessageInspectors.Add(inspector);
        }

        public void ApplyDispatchBehavior(ServiceEndpoint endpoint, EndpointDispatcher endpointDispatcher)
        {
            // No dispatch behavior needed
        }

        public void Validate(ServiceEndpoint endpoint)
        {
            // No validation needed
        }
    }

    public class CustomMessageInspector : IClientMessageInspector
    {
        public string Request { get; private set; }
        public string Reply { get; private set; }

        public object BeforeSendRequest(ref System.ServiceModel.Channels.Message request, System.ServiceModel.IClientChannel channel)
        {
            // Retrieve the outgoing SOAP message
            Request = request.ToString();

            // Return null or any other object that will be passed back to the AfterReceiveReply method
            return null;
        }

        public void AfterReceiveReply(ref System.ServiceModel.Channels.Message reply, object correlationState)
        {
            // Retrieve the incoming SOAP message
            Reply = reply.ToString();
        }
    }
}
