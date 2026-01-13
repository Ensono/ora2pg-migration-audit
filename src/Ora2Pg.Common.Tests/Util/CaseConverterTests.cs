using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Ora2Pg.Common.Util;
using Ora2Pg.Common.Connection;

namespace Ora2Pg.Common.Tests.Util
{
    public class CaseConverterTests
    {
        [Theory]
        [InlineAutoNSubstituteData("my_schema")]
        public void NormalizeSchemaName_Oracle_ToUpper(string schema)
        {
            // Act
            var result = CaseConverter.NormalizeSchemaName(schema, DatabaseType.Oracle);

            // Assert
            Assert.Equal(schema.ToUpper(), result);
        }
    }
}