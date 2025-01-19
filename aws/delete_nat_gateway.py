import boto3
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ToggleNATGatewayForDatabricksWorkspaceTrait:
    """
    Base class for managing NAT Gateway operations in a Databricks workspace VPC.
    Handles initialization of AWS client and finding necessary AWS resources.
    """
    def __init__(self, profile_name: str, workspace_id: str, region_name: str = 'eu-central-1'):
        """
        Initialize the trait with AWS credentials and Databricks workspace information.
        """
        self.client = boto3.Session(profile_name=profile_name).client('ec2', region_name=region_name)
        self.vpc_id = self._find_vpc_id_by_name(workspace_id)
        self.route_table = self._find_default_route_table_by_vpcid(self.vpc_id)
        self.subnet_id_for_natgw = self._find_subnet_id_for_natgw_by_vpc_id(self.vpc_id)

    def _find_vpc_id_by_name(self, workspace_id: str):
        """
        Find the VPC ID associated with the Databricks workspace.
        """
        response = self.client.describe_vpcs()
        vpcs = response['Vpcs']
        vpc_list = []
        
        for vpc in vpcs:
            tags = vpc.get('Tags', [])
            for tag in tags:
                if tag['Key'] == 'Name' and f'workerenv-{workspace_id}' in tag['Value']:
                    vpc_list.append(vpc['VpcId'])
        
        if len(vpc_list) == 1:
            return vpc_list[0]
        elif len(vpc_list) > 1:
            raise ValueError(f"{len(vpc_list)} VPCs found with name containing workspace ID '{workspace_id}'")
        else:
            raise ValueError(f"No VPC found with name containing workspace ID '{workspace_id}'")

    def _find_default_route_table_by_vpcid(self, vpc_id: str) -> str:
        """
        Find the default route table for the given VPC.
        """
        filter = [
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "association.main", "Values": ["true"]},
        ]
        response = self.client.describe_route_tables(Filters=filter)
        return response["RouteTables"][0]["RouteTableId"]

    def _find_subnet_id_for_natgw_by_vpc_id(self, vpc_id: str) -> str:
        """
        Find the subnet designated for NAT Gateway in the VPC.
        """
        filter = [
            {"Name": "vpc-id", "Values": [vpc_id]},
        ]
        response = self.client.describe_subnets(Filters=filter)
        matched_subnets = []

        for subnet in response['Subnets']:
            for tag in subnet.get('Tags', []):
                if tag['Key'] == 'Name' and 'nat-gateway-subnet' in tag['Value']:
                    matched_subnets.append(subnet)

        if (cnt := len(matched_subnets)) != 1:
            raise ValueError(f"{cnt} subnets found with name 'nat-gateway-subnet' in VPC '{vpc_id}'")

        return matched_subnets[0]['SubnetId']


class DeleteNATGateway(ToggleNATGatewayForDatabricksWorkspaceTrait):
    """
    Class for deleting the NAT Gateway in the Databricks workspace VPC.
    Inherits from ToggleNATGatewayForDatabricksWorkspaceTrait.
    """
    def __init__(self, profile_name: str, workspace_id: str, region_name: str = 'eu-central-1'):
        """
        Initialize the NAT Gateway deleter.
        """
        super().__init__(profile_name, workspace_id, region_name)
        self.natgw_id = self._find_natgw_id_by_subnet_id()
        self.eip_association_id = self._find_eip_association_id_by_natgw_id()

    def _find_natgw_id_by_subnet_id(self) -> str:
        """
        Find the NAT Gateway ID associated with the subnet.
        """
        filter = [
            {"Name": "subnet-id", "Values": [self.subnet_id_for_natgw]},
            {"Name": "state", "Values": ["available"]},
        ]
        response = self.client.describe_nat_gateways(Filters=filter)
        if (cnt := len(response['NatGateways'])) != 1:
            raise ValueError(f"{cnt} NatGateways found in subnet '{self.subnet_id_for_natgw}'")
        return response['NatGateways'][0]['NatGatewayId']

    def _find_eip_association_id_by_natgw_id(self) -> str:
        """
        Find the Elastic IP association ID for the NAT Gateway.
        """
        filter = [
            {"Name": "nat-gateway-id", "Values": [self.natgw_id]},
        ]
        response = self.client.describe_nat_gateways(Filters=filter)
        if (cnt := len(response['NatGateways'])) != 1:
            raise ValueError(f"{cnt} NatGateways found in subnet '{self.subnet_id_for_natgw}'")
        return response['NatGateways'][0]['NatGatewayAddresses'][0]['AllocationId']

    def delete_route_to_natgw(self):
        """
        Delete the route to NAT Gateway (0.0.0.0/0) from the route table.
        Logs success or warning if no route is found.
        """
        try:
            response = self.client.describe_route_tables(RouteTableIds=[self.route_table])
            routes = response['RouteTables'][0]['Routes']
            if any(route.get('DestinationCidrBlock') == '0.0.0.0/0' for route in routes):
                self.client.delete_route(
                    DestinationCidrBlock='0.0.0.0/0',
                    RouteTableId=self.route_table
                )
                logger.info(f"Route to NAT Gateway on {self.route_table} deleted successfully")
            else:
                logger.warning("No route with destination 0.0.0.0/0 found in route table.")
        except Exception as e:
            logger.error(f"Error deleting route to NAT Gateway: {e}")

    def delete_natgw(self):
        """
        Delete the NAT Gateway and wait for deletion to complete.
        """
        response = self.client.delete_nat_gateway(
            NatGatewayId=self.natgw_id
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info(f"NAT Gateway {self.natgw_id} is deleting...")
            waiter = self.client.get_waiter('nat_gateway_deleted')
            waiter.wait(NatGatewayIds=[self.natgw_id])
            logger.info(f"NAT Gateway {self.natgw_id} deleted successfully")
        else:
            raise ValueError(f"Failed to delete NAT Gateway {self.natgw_id}\n{response}")

    def release_eip(self):
        """
        Release the Elastic IP associated with the NAT Gateway.
        """
        response = self.client.release_address(
            AllocationId=self.eip_association_id
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info(f"Elastic IP {self.eip_association_id} released successfully")
        else:
            raise ValueError(f"Failed to release Elastic IP {self.eip_association_id}\n{response}")

    def check_routes_in_route_table(self):
        """
        Log all routes in the route table for debugging purposes.
        Logs destination CIDR blocks and their targets.
        """
        try:
            response = self.client.describe_route_tables(RouteTableIds=[self.route_table])
            for route in response['RouteTables'][0]['Routes']:
                destination = route.get('DestinationCidrBlock', 'N/A')
                target = route.get('NatGatewayId', 'None')
                logger.info(f"Destination: {destination}, Target: {target}")
        except Exception as e:
            logger.error(f"Error retrieving routes in route table: {e}")
    
    def run(self):
        """
        Execute the complete NAT Gateway deletion workflow:
        1. Check current routes in route table
        2. Delete route to NAT Gateway
        3. Delete the NAT Gateway
        4. Release the associated Elastic IP
        """
        self.check_routes_in_route_table()  
        self.delete_route_to_natgw()  
        self.delete_natgw() 
        self.release_eip() 


if __name__ == "__main__":
    profile_name = "default" # Replace with your AWS profile name
    workspace_id = "1018030004293411"   # Replace with your Databricks workspace ID
    region_name = "ap-south-1"           # Specify the AWS region
    deleter = DeleteNATGateway(profile_name, workspace_id, region_name)
    deleter.run()