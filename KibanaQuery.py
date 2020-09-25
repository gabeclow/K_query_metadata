import requests
import sys, os
import csv,json
import tldextract
import ipaddress
​​
#orb_endpoint = "https://metadata-orb-source.search.windows.net/indexes/orb-source-090118/docs?api-version=2017-11-11&$select=start_ip_int,end_ip_int,country_code&$filter=domain eq '{}' and range ge 30"
orb_endpoint = 'http://10.138.206.175:9200/orb_cidr/cidr090118/_search'
​
def clean_domain(dom):
    if dom != '' and dom != None:
        dom = tldextract.extract(dom.strip().lower())
        domain = [dom.subdomain, dom.domain, dom.suffix]
        domain = [ x for x in domain if x != '']
        dom = '.'.join(domain)
        if dom.startswith('www.'):
            dom = dom[4:]
        return dom
    else:
        return dom
​
def listify(value):
    data = []
    for r in value:
        input = r['_source']['null']
        domain = r['_source']['domain']
        input_length = len(input)
        if len(input) == 1:
            continue
        else:
            end = input[input_length-2]
            if input_length-2 == 0:
                start = input[0]
            else:
                start = input[input_length-3]
        start = ipaddress.ip_address(int(start)).__str__()
        end = ipaddress.ip_address(int(end)).__str__()
        data.append([ domain, start, end ])
    return data
​
def query_index(domain, name):
    keep_going = True
    result = []
    SIZE = 200
    #headers  = {'api-key': 'C0C7BAFDCFBE737287B65D7ED6B64D01', 'Accept': 'application/json'}
    es_header = {
            'Content-Type': 'application/json'
        }
    es_data = { "size": SIZE, "query": {
                    "bool": {
                        "filter":[{
                            "match": {
                                "domain.keyword": domain
                            }
                        }
                    ],
            "must": [
                {
                    "range": {
                        "range": {
                            "gte": 29
                        }
                    }
                }
            ]
                }
                }
            }
    if name != '' and name is not None:
        name = name.replace('/','').replace(':','')
        es_data["query"]["bool"]["must"].append({
                  "query_string": {
                    "fields": ["name"],
                    "query": name
                  }
                })
    from_ = 0
#    print(json.dumps(es_data))
    response = requests.post(orb_endpoint, headers=es_header, data=json.dumps(es_data))
    while keep_going:
        if response.status_code == 200:
            value = response.json()['hits']['hits']
            from_ += len(value)
            result = result + listify(value)
            if len(value) > 0:
                es_data['from'] = from_
                response = requests.post(orb_endpoint, headers=es_header, data=json.dumps(es_data))
            else:
                return result
        else:
            print(domain, response.json())
            return result
    return result
​
def find_ips(f):
    print(f)
    reader = csv.reader(open('./to_match/'+f), delimiter=',', quotechar='"')
    writer = csv.writer(open('./results/result_' + f, 'w'), delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
​
    writer.writerow(['domain','start', 'end'])
    visited = set()
    idx = 0
    for line in reader:
#        if idx > 3:
#            break
        try:
            idx += 1
            domain = clean_domain(line[1])
            name = line[0]
            if domain in visited:
                continue
            result = query_index(domain, name)
            if result:
                visited.add(domain)
                writer.writerows(result)
        except Exception as e:
            #logger.error(traceback.format_exc())
            print(e)
            continue
​
for f in os.listdir('./to_match/'):
    find_ips(f)