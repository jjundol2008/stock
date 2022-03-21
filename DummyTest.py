



    url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count={pages_to_fetch + 1}&requestType=0"

    get_result = requests.get(url)
    bs_obj = BeautifulSoup(get_result.content, "html.parser")
