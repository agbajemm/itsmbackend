using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace documentchecker.Migrations
{
    /// <inheritdoc />
    public partial class AddManageEngineRequestsTable : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "ManageEngineRequests",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    Subject = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    CreatedTime = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false),
                    TechnicianName = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    JsonData = table.Column<string>(type: "nvarchar(max)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ManageEngineRequests", x => x.Id);
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "ManageEngineRequests");
        }
    }
}
