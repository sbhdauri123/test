using Amazon.S3;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using Google.Apis.Storage.v1;
using Greenhouse.Auth;
using Greenhouse.Common;
using Greenhouse.Data.Model.Setup;
using Rebex.Net;
using System;
using System.Runtime.Caching;

namespace Greenhouse.Services.RemoteAccess
{
    public static class RemoteClientCache
    {
        private static MemoryCache _cache = new MemoryCache("RemoteClientCache");

        internal static void ResetAll()
        {
            _cache = new MemoryCache("RemoteClientCache");
        }

        internal static object GetItem(Greenhouse.Common.Constants.DeliveryType deliveryType, string region, Credential creds)
        {
            //don't cache for SFTP, it is causing issues - GH-765
            if (deliveryType == Common.Constants.DeliveryType.SFTP)
            {
                return InitRemoteClient(deliveryType, region, creds);
            }
            string compoundKey = string.Format("{0}|{1}|{2}", deliveryType, region, creds == null ? "NULLCREDS" : creds.ConnectionString);
            return GetOrAddExisting(compoundKey, () => InitRemoteClient(deliveryType, region, creds));
        }

        private static T GetOrAddExisting<T>(string key, Func<T> valueFactory)
        {
            var newValue = new Lazy<T>(valueFactory);
            var oldValue = _cache.AddOrGetExisting(key, newValue, new CacheItemPolicy()) as Lazy<T>;
            try
            {
                return (oldValue ?? newValue).Value;
            }
            catch (Exception)
            {
                // Handle cached lazy exception by evicting from cache.
                _cache.Remove(key);
                throw;
            }
        }

        private static object InitRemoteClient(Greenhouse.Common.Constants.DeliveryType deliveryType, string key, Credential creds)
        {
            object retVal = null;

            switch (deliveryType)
            {
                case Common.Constants.DeliveryType.GCS:
                    retVal = InitGCSClient(creds);
                    break;
                case Common.Constants.DeliveryType.SFTP:
                    retVal = InitSFTPClient();
                    break;
                case Common.Constants.DeliveryType.S3:
                    retVal = InitS3Client(creds);
                    break;
            }

            return retVal;
        }

        private static AmazonS3Client InitS3Client(Credential creds)
        {
            AmazonS3Config config = new AmazonS3Config();
            config.Timeout = new TimeSpan(0, 100, 0); //one hundred minutes
            config.RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(creds.CredentialSet.Region);
            config.UseHttp = false;
            Amazon.Runtime.AWSCredentials awsCredentials = new Amazon.Runtime.BasicAWSCredentials(creds.CredentialSet.AccessKey, creds.CredentialSet.SecretKey);

            if ((Constants.CredentialType)creds.CredentialTypeID != Constants.CredentialType.AWS_ASSUMEROLE)
                return new AmazonS3Client(awsCredentials, config);

            //Use AssumeRoleAWSCredentials class to obtain security credentials
            //Pass creds to AWS Client
            //Create AWS STS Client 
            var stsClient = new AmazonSecurityTokenServiceClient(awsCredentials, config.RegionEndpoint);

            //Call AssumeRole Operation to obtain security credentials
            var assumeRoleRequest = new AssumeRoleRequest
            {
                RoleArn = creds.CredentialSet.AuthorizedARN,
                RoleSessionName = "SessionAuthorizedRole_" + creds.CredentialID
            };

            var assumeRoleResponse = stsClient.AssumeRoleAsync(assumeRoleRequest).Result;

            var AssumePartnerRoleClient = new AmazonSecurityTokenServiceClient(assumeRoleResponse.Credentials, config.RegionEndpoint);

            //Call AssumeRole Operation to obtain security credentials
            var AssumePartnerRole = new AssumeRoleRequest
            {
                RoleArn = creds.CredentialSet.PartnerARN,
                RoleSessionName = "SessionAssumedRole_" + creds.CredentialID,
                ExternalId = creds.CredentialSet.ExternalId
            };

            var assumeRoleResponse2 = AssumePartnerRoleClient.AssumeRoleAsync(AssumePartnerRole).Result;
            //Use obtained credentals to create S3 client
            return new AmazonS3Client(assumeRoleResponse2.Credentials, config);
        }

        private static Sftp InitSFTPClient()
        {
            Sftp ftpClient = new Sftp();
            ftpClient.Settings = new SftpSettings();
            ftpClient.Timeout = -1;
            return ftpClient;
        }

        private static StorageService InitGCSClient(Credential creds)
        {
            var authenticator = new GoogleOAuthAuthenticator(creds.CredentialSet.Username, creds.CredentialSet.ClientId, creds.CredentialSet.ClientSecret, creds.CredentialSet.RefreshToken);
            var storageService = new StorageService(new Google.Apis.Services.BaseClientService.Initializer
            {
                HttpClientInitializer = authenticator.GetUserCredential(),
                ApplicationName = "GLANCE"
            });
            return storageService;
        }

    }
}