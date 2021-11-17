using System;
using Microsoft.EntityFrameworkCore.Migrations;

namespace EventHub.Consumer.Migrations
{
    public partial class CreateDatabase : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "Case3",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false),
                    Code = table.Column<string>(type: "text", nullable: true),
                    Ticket = table.Column<long>(type: "bigint", nullable: false),
                    Employee = table.Column<int>(type: "integer", nullable: false),
                    Tax = table.Column<double>(type: "double precision", nullable: false),
                    Date = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Case3", x => x.Id);
                });
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "Case3");
        }
    }
}
