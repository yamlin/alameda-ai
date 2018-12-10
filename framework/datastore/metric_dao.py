# pylint: disable=import-error, no-self-use, unused-argument, invalid-name, no-member
''' The Metric DAO '''
import grpc

from alameda_api.v1alpha1.operator import server_pb2, server_pb2_grpc
from framework.log.logger import Logger
from framework.utils.sys_utils import get_metric_server_address


class MockMetricDAO(object):
    #pylint: disable=line-too-long
    ''' Mock DAO '''
    def __init__(self):
        self.logger = Logger()

    def write_container_recommendation_result(self, recommendations):
        ''' Write the recommendation '''
        self.logger.info("Write recommendation result: %s", str(recommendations))

    def write_container_prediction_data(self, predictions):
        ''' Write predictions '''
        self.logger.info("Write prediction result: %s", str(predictions))

    def get_container_observed_data(self, metric_type, namespace, pod_name, duration):
        # pylint: disable=line-too-long
        ''' Get metrics '''
        self.logger.info("Get observed data: metric_type=%s, "
                         " namespace=%s, pod_name=%s, duration=%s",
                         str(metric_type), str(namespace),
                         str(pod_name), str(duration))
        return [
            {
                "data": [{"time": 1540970511, "value": 0.11933598524417476}, {"time": 1540970541, "value": 0.12222222222222222}, {"time": 1540970571, "value": 0.1211138025288804}, {"time": 1540970601, "value": 0.12064253815904132}, {"time": 1540970631, "value": 0.11799999999994824}, {"time": 1540970661, "value": 0.1175581679593074}, {"time": 1540970691, "value": 0.11978842563782154}, {"time": 1540970721, "value": 0.11999733339262363}, {"time": 1540970751, "value": 0.1177777777777616}, {"time": 1540970781, "value": 0.11555812351387203}, {"time": 1540970811, "value": 0.1153358963532232}, {"time": 1540970841, "value": 0.115775204995506}, {"time": 1540970871, "value": 0.11378536346865305}, {"time": 1540970901, "value": 0.11199751116643794}, {"time": 1540970931, "value": 0.11200248894413732}, {"time": 1540970961, "value": 0.11422222222220929}, {"time": 1540970991, "value": 0.11044444444447031}, {"time": 1540971021, "value": 0.11088642474615233}, {"time": 1540971051, "value": 0.11486591570570893}, {"time": 1540971081, "value": 0.12333607413496463}, {"time": 1540971111, "value": 0.12533611858039997}, {"time": 1540971141, "value": 0.12222222222222222}, {"time": 1540971171, "value": 0.12333333333331715}, {"time": 1540971201, "value": 0.12421670147993745}, {"time": 1540971231, "value": 0.1242498332962913}, {"time": 1540971261, "value": 0.12088083016684867}, {"time": 1540971291, "value": 0.12177236567262706}, {"time": 1540971321, "value": 0.12444444444449294}, {"time": 1540971351, "value": 0.12422774345526791}, {"time": 1540971381, "value": 0.12112187750023842}, {"time": 1540971411, "value": 0.11888888888885656}, {"time": 1540971441, "value": 0.11644703215630572}, {"time": 1540971471, "value": 0.1171111111111208}, {"time": 1540971501, "value": 0.12021687924980788}, {"time": 1540971531, "value": 0.12088888888885978}, {"time": 1540971561, "value": 0.11955289882449321}, {"time": 1540971591, "value": 0.11533845948714005}, {"time": 1540971621, "value": 0.11353284898577133}, {"time": 1540971651, "value": 0.11400253338957268}, {"time": 1540971681, "value": 0.1135378941052935}, {"time": 1540971711, "value": 0.1135732225012909}, {"time": 1540971741, "value": 0.11418923422121205}, {"time": 1540971771, "value": 0.11734376389017855}, {"time": 1540971801, "value": 0.1191164051735762}, {"time": 1540971831, "value": 0.11825112805359857}, {"time": 1540971861, "value": 0.11752682677569645}, {"time": 1540971891, "value": 0.1177777777777616}, {"time": 1540971921, "value": 0.11911640517349537}, {"time": 1540971951, "value": 0.11914553093108997}, {"time": 1540971981, "value": 0.12266666666667637}, {"time": 1540972011, "value": 0.123333333333398}, {"time": 1540972041, "value": 0.1213252449837292}, {"time": 1540972071, "value": 0.1168888888888533}, {"time": 1540972101, "value": 0.11799999999994824}, {"time": 1540972131, "value": 0.12133872616558795}, {"time": 1540972161, "value": 0.12422222222222544}, {"time": 1540972191, "value": 0.12510833092600207}, {"time": 1540972221, "value": 0.12311658295926203}, {"time": 1540972251, "value": 0.12266121505711827}, {"time": 1540972281, "value": 0.12288888888886301}, {"time": 1540972311, "value": 0.1226639408012444}, {"time": 1540972341, "value": 0.12044979776883176}, {"time": 1540972371, "value": 0.11755555555557495}, {"time": 1540972401, "value": 0.11776730957254714}, {"time": 1540972431, "value": 0.11821959512010198}, {"time": 1540972461, "value": 0.11955555555557819}, {"time": 1540972491, "value": 0.1144546181882349}, {"time": 1540972521, "value": 0.1135580790684367}, {"time": 1540972551, "value": 0.11133333333329776}, {"time": 1540972581, "value": 0.11666925931687372}, {"time": 1540972611, "value": 0.1191111111110432}, {"time": 1540972641, "value": 0.12042838732617632}, {"time": 1540972671, "value": 0.12177507166506443}, {"time": 1540972701, "value": 0.12218420935708889}, {"time": 1540972731, "value": 0.11912699471041878}, {"time": 1540972761, "value": 0.11464373791907961}, {"time": 1540972791, "value": 0.11247943804742164}, {"time": 1540972821, "value": 0.11578035067444005}, {"time": 1540972851, "value": 0.1169122713432028}, {"time": 1540972881, "value": 0.11510855314326997}, {"time": 1540972911, "value": 0.10976070389047683}, {"time": 1540972941, "value": 0.10822462721391547}, {"time": 1540972971, "value": 0.10778017289269862}, {"time": 1540973001, "value": 0.11223968172825266}, {"time": 1540973031, "value": 0.11221723478954873}, {"time": 1540973061, "value": 0.11421968400700915}, {"time": 1540973091, "value": 0.11333081487082916}, {"time": 1540973121, "value": 0.11089135314121648}, {"time": 1540973151, "value": 0.10466666666672811}, {"time": 1540973181, "value": 0.10732856317492798}, {"time": 1540973211, "value": 0.10955555555556203}],
                "labels": {"namespace": "default", "pod_name": "router-2-m7vx8", "container_name": "router1"}
            },
            {
                "data": [{"time": 1540970511, "value": 0.11933598524417476}, {"time": 1540970541, "value": 0.12222222222222222}, {"time": 1540970571, "value": 0.1211138025288804}, {"time": 1540970601, "value": 0.12064253815904132}, {"time": 1540970631, "value": 0.11799999999994824}, {"time": 1540970661, "value": 0.1175581679593074}, {"time": 1540970691, "value": 0.11978842563782154}, {"time": 1540970721, "value": 0.11999733339262363}, {"time": 1540970751, "value": 0.1177777777777616}, {"time": 1540970781, "value": 0.11555812351387203}, {"time": 1540970811, "value": 0.1153358963532232}, {"time": 1540970841, "value": 0.115775204995506}, {"time": 1540970871, "value": 0.11378536346865305}, {"time": 1540970901, "value": 0.11199751116643794}, {"time": 1540970931, "value": 0.11200248894413732}, {"time": 1540970961, "value": 0.11422222222220929}, {"time": 1540970991, "value": 0.11044444444447031}, {"time": 1540971021, "value": 0.11088642474615233}, {"time": 1540971051, "value": 0.11486591570570893}, {"time": 1540971081, "value": 0.12333607413496463}, {"time": 1540971111, "value": 0.12533611858039997}, {"time": 1540971141, "value": 0.12222222222222222}, {"time": 1540971171, "value": 0.12333333333331715}, {"time": 1540971201, "value": 0.12421670147993745}, {"time": 1540971231, "value": 0.1242498332962913}, {"time": 1540971261, "value": 0.12088083016684867}, {"time": 1540971291, "value": 0.12177236567262706}, {"time": 1540971321, "value": 0.12444444444449294}, {"time": 1540971351, "value": 0.12422774345526791}, {"time": 1540971381, "value": 0.12112187750023842}, {"time": 1540971411, "value": 0.11888888888885656}, {"time": 1540971441, "value": 0.11644703215630572}, {"time": 1540971471, "value": 0.1171111111111208}, {"time": 1540971501, "value": 0.12021687924980788}, {"time": 1540971531, "value": 0.12088888888885978}, {"time": 1540971561, "value": 0.11955289882449321}, {"time": 1540971591, "value": 0.11533845948714005}, {"time": 1540971621, "value": 0.11353284898577133}, {"time": 1540971651, "value": 0.11400253338957268}, {"time": 1540971681, "value": 0.1135378941052935}, {"time": 1540971711, "value": 0.1135732225012909}, {"time": 1540971741, "value": 0.11418923422121205}, {"time": 1540971771, "value": 0.11734376389017855}, {"time": 1540971801, "value": 0.1191164051735762}, {"time": 1540971831, "value": 0.11825112805359857}, {"time": 1540971861, "value": 0.11752682677569645}, {"time": 1540971891, "value": 0.1177777777777616}, {"time": 1540971921, "value": 0.11911640517349537}, {"time": 1540971951, "value": 0.11914553093108997}, {"time": 1540971981, "value": 0.12266666666667637}, {"time": 1540972011, "value": 0.123333333333398}, {"time": 1540972041, "value": 0.1213252449837292}, {"time": 1540972071, "value": 0.1168888888888533}, {"time": 1540972101, "value": 0.11799999999994824}, {"time": 1540972131, "value": 0.12133872616558795}, {"time": 1540972161, "value": 0.12422222222222544}, {"time": 1540972191, "value": 0.12510833092600207}, {"time": 1540972221, "value": 0.12311658295926203}, {"time": 1540972251, "value": 0.12266121505711827}, {"time": 1540972281, "value": 0.12288888888886301}, {"time": 1540972311, "value": 0.1226639408012444}, {"time": 1540972341, "value": 0.12044979776883176}, {"time": 1540972371, "value": 0.11755555555557495}, {"time": 1540972401, "value": 0.11776730957254714}, {"time": 1540972431, "value": 0.11821959512010198}, {"time": 1540972461, "value": 0.11955555555557819}, {"time": 1540972491, "value": 0.1144546181882349}, {"time": 1540972521, "value": 0.1135580790684367}, {"time": 1540972551, "value": 0.11133333333329776}, {"time": 1540972581, "value": 0.11666925931687372}, {"time": 1540972611, "value": 0.1191111111110432}, {"time": 1540972641, "value": 0.12042838732617632}, {"time": 1540972671, "value": 0.12177507166506443}, {"time": 1540972701, "value": 0.12218420935708889}, {"time": 1540972731, "value": 0.11912699471041878}, {"time": 1540972761, "value": 0.11464373791907961}, {"time": 1540972791, "value": 0.11247943804742164}, {"time": 1540972821, "value": 0.11578035067444005}, {"time": 1540972851, "value": 0.1169122713432028}, {"time": 1540972881, "value": 0.11510855314326997}, {"time": 1540972911, "value": 0.10976070389047683}, {"time": 1540972941, "value": 0.10822462721391547}, {"time": 1540972971, "value": 0.10778017289269862}, {"time": 1540973001, "value": 0.11223968172825266}, {"time": 1540973031, "value": 0.11221723478954873}, {"time": 1540973061, "value": 0.11421968400700915}, {"time": 1540973091, "value": 0.11333081487082916}, {"time": 1540973121, "value": 0.11089135314121648}, {"time": 1540973151, "value": 0.10466666666672811}, {"time": 1540973181, "value": 0.10732856317492798}, {"time": 1540973211, "value": 0.10955555555556203}],
                "labels": {"namespace": "default", "pod_name": "router-2-m7vx8", "container_name": "router2"}
            }
        ]

    def get_node_observed_data(self, metric_type, duration):
        """
        Query node observed data
        :param metric_type: cpu/memory
        :param duration: in seconds
        :return:
        """
        return [
            {
                "data": [{"time": 1540970511, "value": 0.11933598524417476}, {"time": 1540970541, "value": 0.12222222222222222}, {"time": 1540970571, "value": 0.1211138025288804}, {"time": 1540970601, "value": 0.12064253815904132}, {"time": 1540970631, "value": 0.11799999999994824}, {"time": 1540970661, "value": 0.1175581679593074}, {"time": 1540970691, "value": 0.11978842563782154}, {"time": 1540970721, "value": 0.11999733339262363}, {"time": 1540970751, "value": 0.1177777777777616}, {"time": 1540970781, "value": 0.11555812351387203}, {"time": 1540970811, "value": 0.1153358963532232}, {"time": 1540970841, "value": 0.115775204995506}, {"time": 1540970871, "value": 0.11378536346865305}, {"time": 1540970901, "value": 0.11199751116643794}, {"time": 1540970931, "value": 0.11200248894413732}, {"time": 1540970961, "value": 0.11422222222220929}, {"time": 1540970991, "value": 0.11044444444447031}, {"time": 1540971021, "value": 0.11088642474615233}, {"time": 1540971051, "value": 0.11486591570570893}, {"time": 1540971081, "value": 0.12333607413496463}, {"time": 1540971111, "value": 0.12533611858039997}, {"time": 1540971141, "value": 0.12222222222222222}, {"time": 1540971171, "value": 0.12333333333331715}, {"time": 1540971201, "value": 0.12421670147993745}, {"time": 1540971231, "value": 0.1242498332962913}, {"time": 1540971261, "value": 0.12088083016684867}, {"time": 1540971291, "value": 0.12177236567262706}, {"time": 1540971321, "value": 0.12444444444449294}, {"time": 1540971351, "value": 0.12422774345526791}, {"time": 1540971381, "value": 0.12112187750023842}, {"time": 1540971411, "value": 0.11888888888885656}, {"time": 1540971441, "value": 0.11644703215630572}, {"time": 1540971471, "value": 0.1171111111111208}, {"time": 1540971501, "value": 0.12021687924980788}, {"time": 1540971531, "value": 0.12088888888885978}, {"time": 1540971561, "value": 0.11955289882449321}, {"time": 1540971591, "value": 0.11533845948714005}, {"time": 1540971621, "value": 0.11353284898577133}, {"time": 1540971651, "value": 0.11400253338957268}, {"time": 1540971681, "value": 0.1135378941052935}, {"time": 1540971711, "value": 0.1135732225012909}, {"time": 1540971741, "value": 0.11418923422121205}, {"time": 1540971771, "value": 0.11734376389017855}, {"time": 1540971801, "value": 0.1191164051735762}, {"time": 1540971831, "value": 0.11825112805359857}, {"time": 1540971861, "value": 0.11752682677569645}, {"time": 1540971891, "value": 0.1177777777777616}, {"time": 1540971921, "value": 0.11911640517349537}, {"time": 1540971951, "value": 0.11914553093108997}, {"time": 1540971981, "value": 0.12266666666667637}, {"time": 1540972011, "value": 0.123333333333398}, {"time": 1540972041, "value": 0.1213252449837292}, {"time": 1540972071, "value": 0.1168888888888533}, {"time": 1540972101, "value": 0.11799999999994824}, {"time": 1540972131, "value": 0.12133872616558795}, {"time": 1540972161, "value": 0.12422222222222544}, {"time": 1540972191, "value": 0.12510833092600207}, {"time": 1540972221, "value": 0.12311658295926203}, {"time": 1540972251, "value": 0.12266121505711827}, {"time": 1540972281, "value": 0.12288888888886301}, {"time": 1540972311, "value": 0.1226639408012444}, {"time": 1540972341, "value": 0.12044979776883176}, {"time": 1540972371, "value": 0.11755555555557495}, {"time": 1540972401, "value": 0.11776730957254714}, {"time": 1540972431, "value": 0.11821959512010198}, {"time": 1540972461, "value": 0.11955555555557819}, {"time": 1540972491, "value": 0.1144546181882349}, {"time": 1540972521, "value": 0.1135580790684367}, {"time": 1540972551, "value": 0.11133333333329776}, {"time": 1540972581, "value": 0.11666925931687372}, {"time": 1540972611, "value": 0.1191111111110432}, {"time": 1540972641, "value": 0.12042838732617632}, {"time": 1540972671, "value": 0.12177507166506443}, {"time": 1540972701, "value": 0.12218420935708889}, {"time": 1540972731, "value": 0.11912699471041878}, {"time": 1540972761, "value": 0.11464373791907961}, {"time": 1540972791, "value": 0.11247943804742164}, {"time": 1540972821, "value": 0.11578035067444005}, {"time": 1540972851, "value": 0.1169122713432028}, {"time": 1540972881, "value": 0.11510855314326997}, {"time": 1540972911, "value": 0.10976070389047683}, {"time": 1540972941, "value": 0.10822462721391547}, {"time": 1540972971, "value": 0.10778017289269862}, {"time": 1540973001, "value": 0.11223968172825266}, {"time": 1540973031, "value": 0.11221723478954873}, {"time": 1540973061, "value": 0.11421968400700915}, {"time": 1540973091, "value": 0.11333081487082916}, {"time": 1540973121, "value": 0.11089135314121648}, {"time": 1540973151, "value": 0.10466666666672811}, {"time": 1540973181, "value": 0.10732856317492798}, {"time": 1540973211, "value": 0.10955555555556203}],
                "labels": {"node_name": "ip-172-31-17-113.us-west-2.compute.internal"}
            },
            {
                "data": [{"time": 1540970511, "value": 0.11933598524417476}, {"time": 1540970541, "value": 0.12222222222222222}, {"time": 1540970571, "value": 0.1211138025288804}, {"time": 1540970601, "value": 0.12064253815904132}, {"time": 1540970631, "value": 0.11799999999994824}, {"time": 1540970661, "value": 0.1175581679593074}, {"time": 1540970691, "value": 0.11978842563782154}, {"time": 1540970721, "value": 0.11999733339262363}, {"time": 1540970751, "value": 0.1177777777777616}, {"time": 1540970781, "value": 0.11555812351387203}, {"time": 1540970811, "value": 0.1153358963532232}, {"time": 1540970841, "value": 0.115775204995506}, {"time": 1540970871, "value": 0.11378536346865305}, {"time": 1540970901, "value": 0.11199751116643794}, {"time": 1540970931, "value": 0.11200248894413732}, {"time": 1540970961, "value": 0.11422222222220929}, {"time": 1540970991, "value": 0.11044444444447031}, {"time": 1540971021, "value": 0.11088642474615233}, {"time": 1540971051, "value": 0.11486591570570893}, {"time": 1540971081, "value": 0.12333607413496463}, {"time": 1540971111, "value": 0.12533611858039997}, {"time": 1540971141, "value": 0.12222222222222222}, {"time": 1540971171, "value": 0.12333333333331715}, {"time": 1540971201, "value": 0.12421670147993745}, {"time": 1540971231, "value": 0.1242498332962913}, {"time": 1540971261, "value": 0.12088083016684867}, {"time": 1540971291, "value": 0.12177236567262706}, {"time": 1540971321, "value": 0.12444444444449294}, {"time": 1540971351, "value": 0.12422774345526791}, {"time": 1540971381, "value": 0.12112187750023842}, {"time": 1540971411, "value": 0.11888888888885656}, {"time": 1540971441, "value": 0.11644703215630572}, {"time": 1540971471, "value": 0.1171111111111208}, {"time": 1540971501, "value": 0.12021687924980788}, {"time": 1540971531, "value": 0.12088888888885978}, {"time": 1540971561, "value": 0.11955289882449321}, {"time": 1540971591, "value": 0.11533845948714005}, {"time": 1540971621, "value": 0.11353284898577133}, {"time": 1540971651, "value": 0.11400253338957268}, {"time": 1540971681, "value": 0.1135378941052935}, {"time": 1540971711, "value": 0.1135732225012909}, {"time": 1540971741, "value": 0.11418923422121205}, {"time": 1540971771, "value": 0.11734376389017855}, {"time": 1540971801, "value": 0.1191164051735762}, {"time": 1540971831, "value": 0.11825112805359857}, {"time": 1540971861, "value": 0.11752682677569645}, {"time": 1540971891, "value": 0.1177777777777616}, {"time": 1540971921, "value": 0.11911640517349537}, {"time": 1540971951, "value": 0.11914553093108997}, {"time": 1540971981, "value": 0.12266666666667637}, {"time": 1540972011, "value": 0.123333333333398}, {"time": 1540972041, "value": 0.1213252449837292}, {"time": 1540972071, "value": 0.1168888888888533}, {"time": 1540972101, "value": 0.11799999999994824}, {"time": 1540972131, "value": 0.12133872616558795}, {"time": 1540972161, "value": 0.12422222222222544}, {"time": 1540972191, "value": 0.12510833092600207}, {"time": 1540972221, "value": 0.12311658295926203}, {"time": 1540972251, "value": 0.12266121505711827}, {"time": 1540972281, "value": 0.12288888888886301}, {"time": 1540972311, "value": 0.1226639408012444}, {"time": 1540972341, "value": 0.12044979776883176}, {"time": 1540972371, "value": 0.11755555555557495}, {"time": 1540972401, "value": 0.11776730957254714}, {"time": 1540972431, "value": 0.11821959512010198}, {"time": 1540972461, "value": 0.11955555555557819}, {"time": 1540972491, "value": 0.1144546181882349}, {"time": 1540972521, "value": 0.1135580790684367}, {"time": 1540972551, "value": 0.11133333333329776}, {"time": 1540972581, "value": 0.11666925931687372}, {"time": 1540972611, "value": 0.1191111111110432}, {"time": 1540972641, "value": 0.12042838732617632}, {"time": 1540972671, "value": 0.12177507166506443}, {"time": 1540972701, "value": 0.12218420935708889}, {"time": 1540972731, "value": 0.11912699471041878}, {"time": 1540972761, "value": 0.11464373791907961}, {"time": 1540972791, "value": 0.11247943804742164}, {"time": 1540972821, "value": 0.11578035067444005}, {"time": 1540972851, "value": 0.1169122713432028}, {"time": 1540972881, "value": 0.11510855314326997}, {"time": 1540972911, "value": 0.10976070389047683}, {"time": 1540972941, "value": 0.10822462721391547}, {"time": 1540972971, "value": 0.10778017289269862}, {"time": 1540973001, "value": 0.11223968172825266}, {"time": 1540973031, "value": 0.11221723478954873}, {"time": 1540973061, "value": 0.11421968400700915}, {"time": 1540973091, "value": 0.11333081487082916}, {"time": 1540973121, "value": 0.11089135314121648}, {"time": 1540973151, "value": 0.10466666666672811}, {"time": 1540973181, "value": 0.10732856317492798}, {"time": 1540973211, "value": 0.10955555555556203}],
                "labels": {"node_name": "ip-172-31-20-194.us-west-2.compute.internal"}
            }
        ]

    def get_container_init_data(self, metric_type, namespace, pod_name, duration):
        """
        Query container observed data from the time pod started to time T
        :param metric_type: cpu/memory
        :param namespace: namespace of pod
        :param pod_name: name of pod
        :param duration: time T
        :return:
        """
        return [
            {
                "data": [{"time": 1540970511, "value": 0.11933598524417476}, {"time": 1540970541, "value": 0.12222222222222222}, {"time": 1540970571, "value": 0.1211138025288804}, {"time": 1540970601, "value": 0.12064253815904132}, {"time": 1540970631, "value": 0.11799999999994824}, {"time": 1540970661, "value": 0.1175581679593074}, {"time": 1540970691, "value": 0.11978842563782154}, {"time": 1540970721, "value": 0.11999733339262363}, {"time": 1540970751, "value": 0.1177777777777616}, {"time": 1540970781, "value": 0.11555812351387203}, {"time": 1540970811, "value": 0.1153358963532232}, {"time": 1540970841, "value": 0.115775204995506}, {"time": 1540970871, "value": 0.11378536346865305}, {"time": 1540970901, "value": 0.11199751116643794}, {"time": 1540970931, "value": 0.11200248894413732}, {"time": 1540970961, "value": 0.11422222222220929}, {"time": 1540970991, "value": 0.11044444444447031}, {"time": 1540971021, "value": 0.11088642474615233}, {"time": 1540971051, "value": 0.11486591570570893}, {"time": 1540971081, "value": 0.12333607413496463}, {"time": 1540971111, "value": 0.12533611858039997}, {"time": 1540971141, "value": 0.12222222222222222}, {"time": 1540971171, "value": 0.12333333333331715}, {"time": 1540971201, "value": 0.12421670147993745}, {"time": 1540971231, "value": 0.1242498332962913}, {"time": 1540971261, "value": 0.12088083016684867}, {"time": 1540971291, "value": 0.12177236567262706}, {"time": 1540971321, "value": 0.12444444444449294}, {"time": 1540971351, "value": 0.12422774345526791}, {"time": 1540971381, "value": 0.12112187750023842}, {"time": 1540971411, "value": 0.11888888888885656}, {"time": 1540971441, "value": 0.11644703215630572}, {"time": 1540971471, "value": 0.1171111111111208}, {"time": 1540971501, "value": 0.12021687924980788}, {"time": 1540971531, "value": 0.12088888888885978}, {"time": 1540971561, "value": 0.11955289882449321}, {"time": 1540971591, "value": 0.11533845948714005}, {"time": 1540971621, "value": 0.11353284898577133}, {"time": 1540971651, "value": 0.11400253338957268}, {"time": 1540971681, "value": 0.1135378941052935}, {"time": 1540971711, "value": 0.1135732225012909}, {"time": 1540971741, "value": 0.11418923422121205}, {"time": 1540971771, "value": 0.11734376389017855}, {"time": 1540971801, "value": 0.1191164051735762}, {"time": 1540971831, "value": 0.11825112805359857}, {"time": 1540971861, "value": 0.11752682677569645}, {"time": 1540971891, "value": 0.1177777777777616}, {"time": 1540971921, "value": 0.11911640517349537}, {"time": 1540971951, "value": 0.11914553093108997}, {"time": 1540971981, "value": 0.12266666666667637}, {"time": 1540972011, "value": 0.123333333333398}, {"time": 1540972041, "value": 0.1213252449837292}, {"time": 1540972071, "value": 0.1168888888888533}, {"time": 1540972101, "value": 0.11799999999994824}, {"time": 1540972131, "value": 0.12133872616558795}, {"time": 1540972161, "value": 0.12422222222222544}, {"time": 1540972191, "value": 0.12510833092600207}, {"time": 1540972221, "value": 0.12311658295926203}, {"time": 1540972251, "value": 0.12266121505711827}, {"time": 1540972281, "value": 0.12288888888886301}, {"time": 1540972311, "value": 0.1226639408012444}, {"time": 1540972341, "value": 0.12044979776883176}, {"time": 1540972371, "value": 0.11755555555557495}, {"time": 1540972401, "value": 0.11776730957254714}, {"time": 1540972431, "value": 0.11821959512010198}, {"time": 1540972461, "value": 0.11955555555557819}, {"time": 1540972491, "value": 0.1144546181882349}, {"time": 1540972521, "value": 0.1135580790684367}, {"time": 1540972551, "value": 0.11133333333329776}, {"time": 1540972581, "value": 0.11666925931687372}, {"time": 1540972611, "value": 0.1191111111110432}, {"time": 1540972641, "value": 0.12042838732617632}, {"time": 1540972671, "value": 0.12177507166506443}, {"time": 1540972701, "value": 0.12218420935708889}, {"time": 1540972731, "value": 0.11912699471041878}, {"time": 1540972761, "value": 0.11464373791907961}, {"time": 1540972791, "value": 0.11247943804742164}, {"time": 1540972821, "value": 0.11578035067444005}, {"time": 1540972851, "value": 0.1169122713432028}, {"time": 1540972881, "value": 0.11510855314326997}, {"time": 1540972911, "value": 0.10976070389047683}, {"time": 1540972941, "value": 0.10822462721391547}, {"time": 1540972971, "value": 0.10778017289269862}, {"time": 1540973001, "value": 0.11223968172825266}, {"time": 1540973031, "value": 0.11221723478954873}, {"time": 1540973061, "value": 0.11421968400700915}, {"time": 1540973091, "value": 0.11333081487082916}, {"time": 1540973121, "value": 0.11089135314121648}, {"time": 1540973151, "value": 0.10466666666672811}, {"time": 1540973181, "value": 0.10732856317492798}, {"time": 1540973211, "value": 0.10955555555556203}],
                "labels": {"namespace": "default", "pod_name": "router-2-m7vx8", "container_name": "router1"}
            },
            {
                "data": [{"time": 1540970511, "value": 0.11933598524417476}, {"time": 1540970541, "value": 0.12222222222222222}, {"time": 1540970571, "value": 0.1211138025288804}, {"time": 1540970601, "value": 0.12064253815904132}, {"time": 1540970631, "value": 0.11799999999994824}, {"time": 1540970661, "value": 0.1175581679593074}, {"time": 1540970691, "value": 0.11978842563782154}, {"time": 1540970721, "value": 0.11999733339262363}, {"time": 1540970751, "value": 0.1177777777777616}, {"time": 1540970781, "value": 0.11555812351387203}, {"time": 1540970811, "value": 0.1153358963532232}, {"time": 1540970841, "value": 0.115775204995506}, {"time": 1540970871, "value": 0.11378536346865305}, {"time": 1540970901, "value": 0.11199751116643794}, {"time": 1540970931, "value": 0.11200248894413732}, {"time": 1540970961, "value": 0.11422222222220929}, {"time": 1540970991, "value": 0.11044444444447031}, {"time": 1540971021, "value": 0.11088642474615233}, {"time": 1540971051, "value": 0.11486591570570893}, {"time": 1540971081, "value": 0.12333607413496463}, {"time": 1540971111, "value": 0.12533611858039997}, {"time": 1540971141, "value": 0.12222222222222222}, {"time": 1540971171, "value": 0.12333333333331715}, {"time": 1540971201, "value": 0.12421670147993745}, {"time": 1540971231, "value": 0.1242498332962913}, {"time": 1540971261, "value": 0.12088083016684867}, {"time": 1540971291, "value": 0.12177236567262706}, {"time": 1540971321, "value": 0.12444444444449294}, {"time": 1540971351, "value": 0.12422774345526791}, {"time": 1540971381, "value": 0.12112187750023842}, {"time": 1540971411, "value": 0.11888888888885656}, {"time": 1540971441, "value": 0.11644703215630572}, {"time": 1540971471, "value": 0.1171111111111208}, {"time": 1540971501, "value": 0.12021687924980788}, {"time": 1540971531, "value": 0.12088888888885978}, {"time": 1540971561, "value": 0.11955289882449321}, {"time": 1540971591, "value": 0.11533845948714005}, {"time": 1540971621, "value": 0.11353284898577133}, {"time": 1540971651, "value": 0.11400253338957268}, {"time": 1540971681, "value": 0.1135378941052935}, {"time": 1540971711, "value": 0.1135732225012909}, {"time": 1540971741, "value": 0.11418923422121205}, {"time": 1540971771, "value": 0.11734376389017855}, {"time": 1540971801, "value": 0.1191164051735762}, {"time": 1540971831, "value": 0.11825112805359857}, {"time": 1540971861, "value": 0.11752682677569645}, {"time": 1540971891, "value": 0.1177777777777616}, {"time": 1540971921, "value": 0.11911640517349537}, {"time": 1540971951, "value": 0.11914553093108997}, {"time": 1540971981, "value": 0.12266666666667637}, {"time": 1540972011, "value": 0.123333333333398}, {"time": 1540972041, "value": 0.1213252449837292}, {"time": 1540972071, "value": 0.1168888888888533}, {"time": 1540972101, "value": 0.11799999999994824}, {"time": 1540972131, "value": 0.12133872616558795}, {"time": 1540972161, "value": 0.12422222222222544}, {"time": 1540972191, "value": 0.12510833092600207}, {"time": 1540972221, "value": 0.12311658295926203}, {"time": 1540972251, "value": 0.12266121505711827}, {"time": 1540972281, "value": 0.12288888888886301}, {"time": 1540972311, "value": 0.1226639408012444}, {"time": 1540972341, "value": 0.12044979776883176}, {"time": 1540972371, "value": 0.11755555555557495}, {"time": 1540972401, "value": 0.11776730957254714}, {"time": 1540972431, "value": 0.11821959512010198}, {"time": 1540972461, "value": 0.11955555555557819}, {"time": 1540972491, "value": 0.1144546181882349}, {"time": 1540972521, "value": 0.1135580790684367}, {"time": 1540972551, "value": 0.11133333333329776}, {"time": 1540972581, "value": 0.11666925931687372}, {"time": 1540972611, "value": 0.1191111111110432}, {"time": 1540972641, "value": 0.12042838732617632}, {"time": 1540972671, "value": 0.12177507166506443}, {"time": 1540972701, "value": 0.12218420935708889}, {"time": 1540972731, "value": 0.11912699471041878}, {"time": 1540972761, "value": 0.11464373791907961}, {"time": 1540972791, "value": 0.11247943804742164}, {"time": 1540972821, "value": 0.11578035067444005}, {"time": 1540972851, "value": 0.1169122713432028}, {"time": 1540972881, "value": 0.11510855314326997}, {"time": 1540972911, "value": 0.10976070389047683}, {"time": 1540972941, "value": 0.10822462721391547}, {"time": 1540972971, "value": 0.10778017289269862}, {"time": 1540973001, "value": 0.11223968172825266}, {"time": 1540973031, "value": 0.11221723478954873}, {"time": 1540973061, "value": 0.11421968400700915}, {"time": 1540973091, "value": 0.11333081487082916}, {"time": 1540973121, "value": 0.11089135314121648}, {"time": 1540973151, "value": 0.10466666666672811}, {"time": 1540973181, "value": 0.10732856317492798}, {"time": 1540973211, "value": 0.10955555555556203}],
                "labels": {"namespace": "default", "pod_name": "router-2-m7vx8", "container_name": "router2"}
            }
        ]

    def get_container_init_resource(self, namespace, pod_name):
        """
        Get init_resource from previous recommendation results by container
        :param namespace: namespace of pod
        :param pod_name: name of pod
        :return:
        """
        return [
            {"container_name": "router2",
             "resources": {"limits": {"cpu": "150m", "memory": "5M"}, "requests": {"cpu": "150m", "memory": "5M"}}},
            {"container_name": "router1",
             "resources": {"limits": {"cpu": "150m", "memory": "5M"}, "requests": {"cpu": "150m", "memory": "5M"}}}
        ]

    def get_container_resources(self, namespace, pod_name):
        """
        Get the spec of resources from user setting
        :param namespace: namespace of pod
        :param pod_name: name of pod
        :return:
        """
        return [
            {"container_name": "router2",
             "resources": {"limits": {"cpu": "150m", "memory": "5M"}, "requests": {"cpu": "150m", "memory": "5M"}}},
            {"container_name": "router1",
             "resources": {"limits": {"cpu": "150m", "memory": "5M"}, "requests": {"cpu": "150m", "memory": "5M"}}}
        ]


class MetricDAO(object):
    ''' Metric DAO '''

    def __init__(self, config=None):
        ''' The construct methdo '''
        if not config:
            config = {
                "metric_server": get_metric_server_address()
            }
        self.config = config
        self.logger = Logger()
        self.logger.info("Metric DAO config: %s", str(self.config))

    def __get_client(self):
        ''' Get the grpc client '''
        conn_str = self.config["metric_server"]
        channel = grpc.insecure_channel(conn_str)
        return server_pb2_grpc.OperatorServiceStub(channel)

    def __get_metric_type_value(self, metric_type):
        ''' Get the metric type '''
        if metric_type == "cpu":
            key = "CONTAINER_CPU_USAGE_TOTAL"
        elif metric_type == "cpu_rate":
            key = "CONTAINER_CPU_USAGE_TOTAL_RATE"
        elif metric_type == "memory":
            key = "CONTAINER_MEMORY_USAGE"
        else:
            key = "CONTAINER_CPU_USAGE_TOTAL"

        # default return cpu
        return server_pb2.MetricType.Value(key)

    def __get_op_type_value(self, op_type):
        ''' Get the op type '''
        if op_type == "equal":
            key = "Equal"
        else:
            key = "NotEqual"
        return server_pb2.StrOp.Value(key)

    def __parse_prediction(self, data):
        ''' Parse the prediction data '''
        result = server_pb2.PredictData()
        result.time.FromSeconds(data["time"])
        result.value = data["value"]
        return result

    def __parse_recommendation(self, data):
        ''' Parse recommendation '''
        result = server_pb2.Recommendation()
        result.time.FromSeconds(data["time"])
        result.resource.CopyFrom(self.__parse_resource(data["resources"]))
        return result

    def __parse_time_series(self, data):
        ''' Parse time series data '''
        result = server_pb2.TimeSeriesData()
        predict_data = list(map(self.__parse_prediction, data))
        result.predict_data.extend(predict_data)
        return result

    def __parse_container_prediction_data(self, data):
        ''' Parse the container prediction data '''
        if not data:
            return []
        result = server_pb2.PredictContainer()

        if "container_name" in data:
            result.name = data["container_name"]

        if "raw_predict" in data:
            for k, v in data["raw_predict"].items():
                time_series = self.__parse_time_series(v)
                result.row_predict_data[k].CopyFrom(time_series)

        if "recommendations" in data:
            result.recommendations.extend(list(
                map(self.__parse_recommendation, data["recommendations"])
            ))

        if "init_resource" in data:
            result.initial_resource.CopyFrom(
                self.__parse_resource(data["init_resource"])
            )

        return result

    def __parse_resource(self, data):
        ''' Parse the resource data '''
        resource = server_pb2.Resource()

        for k, v in data["limits"].items():
            resource.limit[k] = v

        for k, v in data["requests"].items():
            resource.request[k] = v

        return resource

    def __parse_sample(self, data):
        ''' Parse the sample data '''
        return {
            "time": data.time.seconds,
            "value": data.value
        }

    def __parse_metrics(self, data):
        ''' Parse the metrics '''
        result = []
        if not data:
            return result

        for d in data:
            r = {"labels": {}}
            for k, v in d.labels.items():
                r["labels"][k] = v

            # add the sample data
            r["data"] = list(
                map(self.__parse_sample, d.samples)
            )
            result.append(r)
        return result

    def write_container_prediction_data(self, prediction):
        ''' Write the prediction result to server. '''
        self.logger.info("Write prediction result: %s", str(prediction))
        req = server_pb2.CreatePredictResultRequest()
        pod = req.predict_pods.add()

        pod.uid = prediction["uid"]
        pod.namespace = prediction["namespace"]
        pod.name = prediction["pod_name"]
        pod.predict_containers.extend(list(
            map(self.__parse_container_prediction_data,
                prediction["containers"]
               )
        ))
        try:
            client = self.__get_client()
            resp = client.CreatePredictResult(req)
            if resp.status.code != 0:
                msg = "Write prediction error [code={}]".format(resp.status.code)
                raise Exception(msg)
        except Exception as e:
            self.logger.error("Could not get metrics: %s", str(e))
            raise e

    def write_container_recommendation_result(self, data):
        ''' Write the container recommendation result '''
        self.write_container_prediction_data(data)

    def get_container_observed_data(self, metric_type, namespace_name, pod_name, duration):
        ''' Get the observed metrics '''
        self.logger.info("Get observed data: metric_type=%s, "
                         " namespace=%s, pod_name=%s, duration=%s",
                         str(metric_type), str(namespace_name),
                         str(pod_name), str(duration))
        req = server_pb2.ListMetricsRequest()
        req.metric_type = self.__get_metric_type_value(metric_type)
        req.duration.seconds = duration
        # setup the query conditions
        namespace = req.conditions.add()
        namespace.key = u"namespace"
        namespace.op = self.__get_op_type_value("equal")
        namespace.value = namespace_name
        pod = req.conditions.add()
        pod.key = u"pod_name"
        pod.op = self.__get_op_type_value("equal")
        pod.value = pod_name

        try:
            client = self.__get_client()
            resp = client.ListMetrics(req)
            if resp.status.code == 0:
                return self.__parse_metrics(resp.metrics)
            else:
                msg = "List metric error [code={}]".format(resp.status.code)
                raise Exception(msg)
        except Exception as e:
            self.logger.error("Could not get metrics: %s", str(e))
            raise e
