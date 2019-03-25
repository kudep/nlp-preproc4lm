import requests
import logging
import traceback
import re
import time 

logger = logging.getLogger(__name__)

# %%
url_pattern = r'(?i)\b((?:(https?|ftp):\/\/|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}\/)(?:[^\s<>]|\(([^\s<>]+|(\([^\s<>]+\)))*\))+(?:\(([^\s<>]+|(\([^\s<>]+\)))*\)|[^\s`!\[\]{};:\'".,<>?\xab\xbb]))'
url_regex = re.compile(url_pattern)
url = "https://speller.yandex.net/services/spellservice.json/checkText"

def send(text):
    time.sleep(0.5)
    # replace urls
    found_urls = url_regex.findall(str(text).strip())
    found_urls = [match[0] for match in found_urls]

    corrected = text
    text = url_regex.sub(" <URL> ", text)  # swap urls
    try:
        r = requests.post(
            url,
            data="text={}".format(text).encode(),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=20,
        )
        if r.status_code == 200:
            for c in reversed(r.json()):
                if c.get("s"):
                    corrected = corrected[: c["pos"]] + c["s"][0] + corrected[c["pos"] + c["len"] :]
        else:
            logger.info(f'status_code == {r.status_code}')

    except Exception:
        logger.error(traceback.format_exc())

    return corrected
