using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace documentchecker.Migrations
{
    /// <inheritdoc />
    public partial class AddUserEmailToChatConversation : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "UserEmail",
                table: "ChatConversations",
                type: "nvarchar(max)",
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "UserEmail",
                table: "ChatConversations");
        }
    }
}
