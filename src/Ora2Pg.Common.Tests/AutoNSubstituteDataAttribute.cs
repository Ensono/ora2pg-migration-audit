using AutoFixture;
using AutoFixture.AutoNSubstitute;
using AutoFixture.Xunit2;

namespace Verisk.Ciam.Api.UnitTests;

public class AutoNSubstituteDataAttribute() : AutoDataAttribute(() =>
{
    var fixture = new Fixture();
    fixture.Customize(new AutoNSubstituteCustomization { ConfigureMembers = true });
    
    return fixture;
});