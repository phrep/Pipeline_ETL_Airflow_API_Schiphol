import re
import logging
from requests import Response

logger = logging.getLogger(__name__)

def next_page(response:Response):
    headers = response.headers
    headers_link = headers.get("link")
    logger.info(f"link = {headers_link}")

    if not headers_link:
        return None

    link_partes = headers_link.split(",")

    regular_exp = r"<(.*)>"

    for link_parte in link_partes:
        if 'rel="next"' in link_parte:
            link = link_parte.split(";")[0]
            link = re.search(regular_exp, link)
            page_link = link.groups()[0]
            page = re.search("page=(.*)", page_link)
            page = page.groups()[0]
            logger.info(f"page: {page}")

            return dict(data={"page":page})
