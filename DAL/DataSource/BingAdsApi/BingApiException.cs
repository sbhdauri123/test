using System;

namespace Greenhouse.DAL.BingAds.Reporting
{
    public class BingApiException : Exception
    {
        public BingApiException(string message) : base(message) { }
        public BingApiException(string message, System.ServiceModel.FaultException<Greenhouse.DAL.BingAds.Reporting.ApiFaultDetail> detail) : base(message, detail) { }
    }
}
