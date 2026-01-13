using AutoFixture.Xunit2;

namespace Verisk.Ciam.Api.UnitTests;

public class InLineAutoNSubstituteDataAttribute(params object[] values)
    : InlineAutoDataAttribute(new AutoNSubstituteDataAttribute(), values);