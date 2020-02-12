curl -XDELETE 's103:9200/auditindex'

# 创建索引
curl -XPUT 'http://s103:9200/auditindex?pretty'

# 创建type的mapping信息
curl -H "Content-Type: application/json" -XPOST 'http://s103:9200/auditindex/audittype/_mapping?pretty' -d '
{
"audittype":{
	"properties":{
		"area":{"type":"keyword"},
		"type":{"type":"keyword"},
		"count":{"type":"long"},
		"time":{"type":"date","format": "yyyy-MM-dd HH:mm:ss"}
		}
    }
}
