namespace Greenhouse.UI.Models
{
    public class Menu
    {
        public string Email { get; set; }
        public string Name { get; set; }
        public List<MenuLinks> MenuLinks { get; set; }
        public bool Admin { get; set; }
    }

    public class MenuLinks
    {
        public string text { get; set; }
        public string url { get; set; }
        public List<MenuItem> items { get; set; }
    }

    public class MenuItem
    {
        public string text { get; set; }
        public string url { get; set; }
    }
}