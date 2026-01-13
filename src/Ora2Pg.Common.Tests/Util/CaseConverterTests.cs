using Ora2Pg.Common.Util;
using Ora2Pg.Common.Connection;
using Verisk.Ciam.Api.UnitTests;

namespace Ora2Pg.Common.Tests.Util
{
    public class CaseConverterTests
    {
        [Theory]
        [InLineAutoNSubstituteData("my_schema")]
        public void NormalizeSchemaName_Oracle_ToUpper(string schema)
        {
            // Act
            var result = CaseConverter.NormalizeSchemaName(schema, DatabaseType.Oracle);

            // Assert
            Assert.Equal(schema.ToUpper(), result);
        }
    }
}