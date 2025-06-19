using System;
using System.ServiceModel.Description;

namespace Greenhouse.DAL.SOAP
{
    public static class Proxy
    {
        public static T CreateWebServiceChannelProxy<T>(Greenhouse.Data.Model.Setup.Credential credential, System.ServiceModel.Channels.Binding binding)
        {
            string endpointAddressUrl = credential.CredentialSet.Endpoint;
            System.ServiceModel.EndpointAddress endpointAddress = new System.ServiceModel.EndpointAddress(endpointAddressUrl);

            System.ServiceModel.ChannelFactory<T> proxyChannelFactory = new System.ServiceModel.ChannelFactory<T>(binding, endpointAddress);
            proxyChannelFactory.Credentials.UserName.UserName = credential.CredentialSet.UserName;
            return proxyChannelFactory.CreateChannel();
        }

        public static T CreateWebServiceChannelProxy<T>(Greenhouse.Data.Model.Setup.Credential credential, System.ServiceModel.Channels.Binding binding, IEndpointBehavior endPointBehavior)
        {
            string endpointAddressUrl = credential.CredentialSet.Endpoint;
            System.ServiceModel.EndpointAddress endpointAddress = new System.ServiceModel.EndpointAddress(endpointAddressUrl);

            System.ServiceModel.ChannelFactory<T> proxyChannelFactory = new System.ServiceModel.ChannelFactory<T>(binding, endpointAddress);
            proxyChannelFactory.Credentials.UserName.UserName = credential.CredentialSet.UserName;
            proxyChannelFactory.Endpoint.EndpointBehaviors.Add(endPointBehavior);

            return proxyChannelFactory.CreateChannel();
        }

        public static System.ServiceModel.Channels.Binding GetDefaultBinding()
        {
            {
                var binding = new System.ServiceModel.BasicHttpsBinding();
                binding.BypassProxyOnLocal = true;
                binding.AllowCookies = false;
                //binding.UseDefaultWebProxy = true;
                binding.TransferMode = System.ServiceModel.TransferMode.Buffered;
                binding.MessageEncoding = System.ServiceModel.WSMessageEncoding.Text;
                binding.MaxReceivedMessageSize = 1000000 * 10;
                binding.Security.Mode = System.ServiceModel.BasicHttpsSecurityMode.Transport;
                binding.Security.Transport = new System.ServiceModel.HttpTransportSecurity();
                binding.Security.Transport.ClientCredentialType = System.ServiceModel.HttpClientCredentialType.Basic;
                binding.SendTimeout = TimeSpan.FromMinutes(10);
                binding.OpenTimeout = TimeSpan.FromMinutes(10);
                binding.CloseTimeout = TimeSpan.FromMinutes(10);
                binding.ReceiveTimeout = TimeSpan.FromMinutes(60);

                return binding;
            }
        }
    }
}
