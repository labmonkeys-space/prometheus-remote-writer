# A tiny SNMP agent so the sandbox has a node with real interfaces: OpenNMS
# collects mib2-interfaces from it, which is what puts ifDescr on the
# onms_resource_attr rows, if_descr on onms_resource_info and the
# onms_resource_ifspeed gauge on the wire. The smoke imports the requisition
# under e2e/opennms/requisitions/ that points at this container.
#
# ifAlias stays empty: net-snmp's released versions do not report a Linux
# interface alias, so the rows carry no ifAlias and the info series no
# if_alias for this node, which is exactly what the docs say about an
# empty value.
FROM alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce
RUN apk add --no-cache net-snmp net-snmp-tools
COPY snmpd.conf /etc/snmp/snmpd.conf
EXPOSE 161/udp
CMD ["snmpd", "-f", "-Lo", "-C", "-c", "/etc/snmp/snmpd.conf"]
