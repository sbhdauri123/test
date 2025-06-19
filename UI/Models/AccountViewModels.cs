using System.ComponentModel.DataAnnotations;

namespace Greenhouse.UI.Models
{
    public abstract class AccountModel
    {
        [Required]
        [DataType(DataType.EmailAddress)]
        [Display(Name = "E-mail Address")]
        public string UserName { get; set; }
    }

    public class ChangePasswordModel : AccountModel
    {
        [Required]
        [DataType(DataType.Password)]
        [Display(Name = "Current password")]
        public string OldPassword { get; set; }

        [RegularExpression(@"(?=^.{8,}$)(?=(?:.*?\d){1})(?=.*[a-z])(?=(?:.*?[A-Z]){1})(?=(?:.*?[!@#$%*()_+^&}{:;?.]){1})(?!.*\s)[0-9a-zA-Z!@#$%*()_+^&]*$", ErrorMessage = "The password must contain at least one alphanumeric character.")]
        [Required]
        [StringLength(100, ErrorMessage = "The {0} must be at least {2} characters long.", MinimumLength = 8)]
        [DataType(DataType.Password)]
        [Display(Name = "New password")]
        public string NewPassword { get; set; }

        [System.ComponentModel.DataAnnotations.Compare("NewPassword", ErrorMessage = "The new password and confirmation password do not match.")]
        [Required]
        [DataType(DataType.Password)]
        [Display(Name = "Confirm")]
        public string ConfirmPassword { get; set; }

        public bool LoggedIn { get; set; }
    }

    public class LoginModel : AccountModel
    {
        [Required]
        [DataType(DataType.Password)]
        [Display(Name = "Password")]
        public string Password { get; set; }
        public string Message { get; set; }
    }

    public class ForgotPasswordModel : AccountModel { }
}
