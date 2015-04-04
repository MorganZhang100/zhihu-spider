#coding=utf-8

'''
This program is test if you can connect to zhihu.
'''
import gzip
import StringIO
import ConfigParser
import requests

def get_content(toUrl,count):
    """ Return the content of given url

        Args:
            toUrl: aim url
            count: index of this connect

        Return:
            content if success
            'Fail' if fail
    """

    cf = ConfigParser.ConfigParser()
    cf.read("config.ini")
    cookie = cf.get("cookie", "cookie")
    tmp = dict(Cookie=cookie)
    '''
    headers = {
        'Cookie': cookie,
        'Host':'www.zhihu.com',
        'Referer':'http://www.zhihu.com/',
        'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36',
        'Accept-Encoding':'gzip'
    }
    req = urllib2.Request(
        url = toUrl,
        headers = headers
    )
    '''
    try:
        req = requests.put(toUrl,cookies = tmp)
    except Exception,e:
        if count % 1 == 0:
            print str(count) + "Error: " +str(e) + "URL: " + toUrl
    '''
    try:
        opener = urllib2.build_opener(urllib2.ProxyHandler())
        urllib2.install_opener(opener)

        page = urllib2.urlopen(req,timeout = 15)

        headers = page.info()
        content = page.read()
    except Exception,e:
        if count % 1 == 0:
            print str(count) + ", Error: " + str(e) + " URL: " + toUrl
        return "FAIL"

    if page.info().get('Content-Encoding') == 'gzip':
        data = StringIO.StringIO(content)
        gz = gzip.GzipFile(fileobj=data)
        content = gz.read()
        gz.close()
    '''
    return req

def main():
    url = 'https://www.zhihu.com/topic'
    index = 19552832
    content = get_content(url,index)
    if content == 'FAIL':
        print "fail"
    else:
        print content.text

if __name__ == "__main__":
    main()
