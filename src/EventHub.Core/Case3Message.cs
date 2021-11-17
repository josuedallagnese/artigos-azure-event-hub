using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace EventHub.Core
{
    public class Case3Message
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public long Id { get; set; }
        public string Code { get; set; }
        public long Ticket { get; set; }
        public int Employee { get; set; }
        public double Tax { get; set; }
        public DateTimeOffset Date { get; set; }
    }
}
