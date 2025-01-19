import time
import boto3
from botocore.exceptions import ClientError

class ToggleNATGatewayForDatabricksWorkspaceTrait:
    """
    Base class for managing NAT Gateway operations in a Databricks workspace VPC.
    Handles initialization of AWS client and finding necessary AWS resources.
    """
    def __init__(self, profile_name: str, workspace_id: str, region_name: str = 'eu-west-1'):
        """
        Initialize the trait with AWS credentials and Databricks workspace information.
        """
        self.client = boto3.Session(profile_name=profile_name).client('ec2', region_name=region_name)
        self.vpc_id = self._find_vpc_id_by_name(workspace_id)
        self.route_table = self._find_default_route_table_by_vpcid(self.vpc_id)
        self.subnet_id_for_natgw = self._find_subnet_id_for_natgw_by_vpc_id(self.vpc_id)

    def _find_vpc_id_by_name(self, workspace_id: str) -> str:
        """
        Find the VPC ID associated with the Databricks workspace.
        """
        response = self.client.describe_vpcs()
        matched_vpcs = []

        for vpc in response['Vpcs']:
            for tag in vpc.get('Tags', []):
                if tag['Key'] == 'Name' and f'workerenv-{workspace_id}' in tag['Value']:
                    matched_vpcs.append(vpc)

        if (cnt := len(matched_vpcs)) != 1:
            raise ValueError(f"{cnt} VPCs found with name containing workspace ID '{workspace_id}'")

        return matched_vpcs[0]['VpcId']

    def _find_default_route_table_by_vpcid(self, vpc_id: str) -> str:
        """
        Find the default route table for the given VPC.
        """ 
        filters = [
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "association.main", "Values": ["true"]},
        ]
        response = self.client.describe_route_tables(Filters=filters)
        return response["RouteTables"][0]["RouteTableId"]

    def _find_subnet_id_for_natgw_by_vpc_id(self, vpc_id: str) -> str:
        """
        Find the subnet designated for NAT Gateway in a VPC.
        """     
        filters = [
            {"Name": "vpc-id", "Values": [vpc_id]},
        ]
        response = self.client.describe_subnets(Filters=filters)
        matched_subnets = []

        for subnet in response['Subnets']:
            for tag in subnet.get('Tags', []):
                if tag['Key'] == 'Name' and 'nat-gateway-subnet' in tag['Value']:
                    matched_subnets.append(subnet)

        if (cnt := len(matched_subnets)) != 1:
            raise ValueError(f"{cnt} subnets found with name 'nat-gateway-subnet' in VPC '{vpc_id}'")

        return matched_subnets[0]['SubnetId']


class CreateNATGateway(ToggleNATGatewayForDatabricksWorkspaceTrait):
    """
    Class for creating and configuring a NAT Gateway in a Databricks workspace VPC.
    Inherits from ToggleNATGatewayForDatabricksWorkspaceTrait.
    """
    def __init__(self, profile_name: str, workspace_id: str, region_name: str = 'eu-central-1'):
        """
        Initialize the NAT Gateway creator.
        """
        super().__init__(profile_name, workspace_id, region_name)

    def create_eip(self) -> str:
        """
        Create an Elastic IP address for the NAT Gateway.
        """
        response = self.client.allocate_address(Domain='vpc')
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(f"Elastic IP {response['AllocationId']} created successfully")
            return response['AllocationId']
        else:
            raise ValueError(f"Failed to create Elastic IP\n{response}")

    def create_natgw(self, eip_association_id: str) -> str:
        """
        Create a NAT Gateway with the allocated Elastic IP.
        """
        response = self.client.create_nat_gateway(
            AllocationId=eip_association_id,
            SubnetId=self.subnet_id_for_natgw
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(f"NAT Gateway {response['NatGateway']['NatGatewayId']} created successfully")
            return response['NatGateway']['NatGatewayId']
        else:
            raise ValueError(f"Failed to create NAT Gateway\n{response}")

    def check_nat_gateway_status(self, nat_gateway_id: str) -> str:
        """
        Check the NAT Gateway status until it becomes available.
        """
        while True:
            try:
                response = self.client.describe_nat_gateways(NatGatewayIds=[nat_gateway_id])
                nat_gateway = response['NatGateways'][0]
                if nat_gateway['State'] == 'available':
                    print(f"NAT Gateway {nat_gateway_id} is available.")
                    return
                elif nat_gateway['State'] == 'pending':
                    print(f"NAT Gateway {nat_gateway_id} is still pending. Waiting...")
                else:
                    raise ValueError(f"NAT Gateway {nat_gateway_id} is in an unexpected state: {nat_gateway['State']}")
            except ClientError as e:
                raise ValueError(f"Failed to describe NAT Gateway {nat_gateway_id}: {e}")

            time.sleep(30)  # Wait for 30 seconds before checking again

    def update_route_table(self, nat_gateway_id: str):
        """
        Update the route table to route all outbound traffic through the NAT Gateway.
        First delete existing route if present, then create new route.
        """
        try:
            # Try to delete existing route if it exists
            self.client.delete_route(
                RouteTableId=self.route_table,
                DestinationCidrBlock='0.0.0.0/0'
            )
            print(f"Deleted existing route for 0.0.0.0/0 from route table {self.route_table}")
        except ClientError as e:
            if 'InvalidRoute.NotFound' in str(e):
                print(f"No existing route found for 0.0.0.0/0 in route table {self.route_table}")
            else:
                raise e

        # Create new route
        response = self.client.create_route(
            DestinationCidrBlock='0.0.0.0/0',
            NatGatewayId=nat_gateway_id,
            RouteTableId=self.route_table
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(f"Route for 0.0.0.0/0 added to route table {self.route_table} successfully.")
        else:
            raise ValueError(f"Failed to add route to route table\n{response}")

    def run(self):
        """
        Execute the complete NAT Gateway creation workflow:
        1. Create Elastic IP
        2. Create NAT Gateway
        3. Wait for NAT Gateway to become available
        4. Update route table
        """
        eip_association_id = self.create_eip()
        natgw_id = self.create_natgw(eip_association_id)
        self.check_nat_gateway_status(natgw_id)             
        self.update_route_table(natgw_id)                   
        return natgw_id


if __name__ == "__main__":
    profile_name = "default" # Replace with your AWS profile name
    workspace_id = "1018030004293411"   # Replace with your Databricks workspace ID
    region_name = "ap-south-1"           # Specify the AWS region
    creator = CreateNATGateway(profile_name, workspace_id, region_name)
    natgw_id = creator.run()
    print(f"NAT Gateway created with ID: {natgw_id}")


