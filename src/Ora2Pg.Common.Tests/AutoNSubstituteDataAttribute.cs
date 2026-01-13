using AutoFixture;
using AutoFixture.AutoNSubstitute;
using AutoFixture.Xunit2;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Verisk.Ciam.Api.Infrastructure.Configuration;
using Verisk.Ciam.Api.Infrastructure.Data.Repositories;

namespace Verisk.Ciam.Api.UnitTests;

public class AutoNSubstituteDataAttribute() : AutoDataAttribute(() =>
{
    var fixture = new Fixture();
    fixture.Customize(new AutoNSubstituteCustomization { ConfigureMembers = true });
    fixture.Customize<ProductModel>(c => c.With(p => p.Exclude, false));
    fixture.Inject(
        Options.Create(new ProductFeatureConfiguration
        {
            IncludeExcludedProducts = false,
            ProductFileSources = fixture.Create<ProductFileSources>()
        })
    );
    
    var memoryCache = new MemoryCache(new MemoryCacheOptions());
    fixture.Inject<IMemoryCache>(memoryCache);
    fixture.Inject(memoryCache); 
    
    return fixture;
});