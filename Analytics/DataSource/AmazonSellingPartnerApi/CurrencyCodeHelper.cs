using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.Data.DataSource.AmazonSellingPartnerApi;
public static class CurrencyCodeHelper
{
    public enum Marketplace
    {
        Canada,
        UnitedStates,
        Mexico,
        Brazil,
        Ireland,
        Spain,
        UnitedKingdom,
        France,
        Belgium,
        Netherlands,
        Germany,
        Italy,
        Sweden,
        Poland,
        SouthAfrica,
        Egypt,
        Turkey,
        SaudiArabia,
        UnitedArabEmirates,
        India,
        Singapore,
        Japan,
        Australia
    }

    public class MarketplaceInfo
    {
        public string Id { get; set; }
        public string Currency { get; set; }
    }

    public static class MarketplaceManager
    {
        private static readonly Dictionary<Marketplace, MarketplaceInfo> marketplaces = new()
        {
            // North America
            { Marketplace.Canada, new MarketplaceInfo { Id = "A2EUQ1WTGCTBG2", Currency = "CAD" } },
            { Marketplace.UnitedStates, new MarketplaceInfo { Id = "ATVPDKIKX0DER", Currency = "USD" } },
            { Marketplace.Mexico, new MarketplaceInfo { Id = "A1AM78C64UM0Y8", Currency = "MXN" } },
            { Marketplace.Brazil, new MarketplaceInfo { Id = "A2Q3Y263D00KWC", Currency = "BRL" } },

            // Europe
            { Marketplace.Ireland, new MarketplaceInfo { Id = "A28R8C7NBKEWEA", Currency = "EUR" } },
            { Marketplace.Spain, new MarketplaceInfo { Id = "A1RKKUPIHCS9HS", Currency = "EUR" } },
            { Marketplace.UnitedKingdom, new MarketplaceInfo { Id = "A1F83G8C2ARO7P", Currency = "GBP" } },
            { Marketplace.France, new MarketplaceInfo { Id = "A13V1IB3VIYZZH", Currency = "EUR" } },
            { Marketplace.Belgium, new MarketplaceInfo { Id = "AMEN7PMS3EDWL", Currency = "EUR" } },
            { Marketplace.Netherlands, new MarketplaceInfo { Id = "A1805IZSGTT6HS", Currency = "EUR" } },
            { Marketplace.Germany, new MarketplaceInfo { Id = "A1PA6795UKMFR9", Currency = "EUR" } },
            { Marketplace.Italy, new MarketplaceInfo { Id = "APJ6JRA9NG5V4", Currency = "EUR" } },
            { Marketplace.Sweden, new MarketplaceInfo { Id = "A2NODRKZP88ZB9", Currency = "SEK" } },
            { Marketplace.Poland, new MarketplaceInfo { Id = "A1C3SOZRARQ6R3", Currency = "PLN" } },

            // Africa
            { Marketplace.SouthAfrica, new MarketplaceInfo { Id = "AE08WJ6YKNBMC", Currency = "ZAR" } },
            { Marketplace.Egypt, new MarketplaceInfo { Id = "ARBP9OOSHTCHU", Currency = "EGP" } },

            // Middle East
            { Marketplace.Turkey, new MarketplaceInfo { Id = "A33AVAJ2PDY3EV", Currency = "TRY" } },
            { Marketplace.SaudiArabia, new MarketplaceInfo { Id = "A17E79C6D8DWNP", Currency = "SAR" } },
            { Marketplace.UnitedArabEmirates, new MarketplaceInfo { Id = "A2VIGQ35RCS4UG", Currency = "AED" } },

            // Asia
            { Marketplace.India, new MarketplaceInfo { Id = "A21TJRUUN4KGV", Currency = "INR" } },
            { Marketplace.Singapore, new MarketplaceInfo { Id = "A19VAU5U5O7RUS", Currency = "SGD" } },
            { Marketplace.Japan, new MarketplaceInfo { Id = "A1VC38T7YXB528", Currency = "JPY" } },

            // Oceania
            { Marketplace.Australia, new MarketplaceInfo { Id = "A39IBJ37TRP1C6", Currency = "AUD" } }
        };

        // Optimized reverse lookup dictionary
        private static readonly Dictionary<string, string> marketplaceIdToCurrency = marketplaces
            .ToDictionary(m => m.Value.Id, m => m.Value.Currency);

        public static string GetCurrencyCode(string marketplaceId)
        {
            return marketplaceIdToCurrency.TryGetValue(marketplaceId, out var currency) ? currency : "Unknown";
        }
    }
}
