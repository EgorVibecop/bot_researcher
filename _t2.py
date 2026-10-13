import asyncio, httpx, matching, tg_source, search
async def m():
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
        tg = await tg_source.fetch_telegram(c, matching.HABR_QUERIES)
        careers = await search.fetch_careers(c)
    ok_tg = [v for v in tg if matching.classify(v["title"])]
    ok_c = [v for v in careers if matching.classify(v["title"])]
    print("tg: собрано", len(tg), "по теме", len(ok_tg))
    for v in ok_tg[:5]: print("   *", v["title"][:70])
    print("careers: собрано", len(careers), "по теме", len(ok_c))
    for v in ok_c[:8]: print("   *", v["company"], "|", v["title"][:60])
asyncio.run(m())
