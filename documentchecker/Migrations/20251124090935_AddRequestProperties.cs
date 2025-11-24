using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace documentchecker.Migrations
{
    /// <inheritdoc />
    public partial class AddRequestProperties : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            // Add columns as nullable first
            migrationBuilder.AddColumn<string>(
                name: "DisplayId",
                table: "ManageEngineRequests",
                type: "nvarchar(max)",
                nullable: true); // Changed to nullable

            migrationBuilder.AddColumn<string>(
                name: "RequesterEmail",
                table: "ManageEngineRequests",
                type: "nvarchar(max)",
                nullable: true); // Changed to nullable

            migrationBuilder.AddColumn<string>(
                name: "RequesterName",
                table: "ManageEngineRequests",
                type: "nvarchar(max)",
                nullable: true); // Changed to nullable

            migrationBuilder.AddColumn<string>(
                name: "Status",
                table: "ManageEngineRequests",
                type: "nvarchar(max)",
                nullable: true); // Changed to nullable
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "DisplayId",
                table: "ManageEngineRequests");

            migrationBuilder.DropColumn(
                name: "RequesterEmail",
                table: "ManageEngineRequests");

            migrationBuilder.DropColumn(
                name: "RequesterName",
                table: "ManageEngineRequests");

            migrationBuilder.DropColumn(
                name: "Status",
                table: "ManageEngineRequests");
        }
    }
}