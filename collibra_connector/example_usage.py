"""
Example usage of the Collibra Python Connector.
This file demonstrates how to use the various APIs provided by the connector.
"""

from collibra_connector import CollibraConnector

# Initialize the connector
connector = CollibraConnector(
    api="https://your-collibra-instance.com",
    username="your-username",
    password="your-password",
    timeout=30
)


def main():
    """Main example function demonstrating various API operations."""

    try:
        # Test connection
        if connector.test_connection():
            print("✅ Connection successful!")
        else:
            print("❌ Connection failed!")
            return

        # 1. Get all metadata
        print("\n📊 Getting Collibra metadata...")
        metadata = connector.metadata.get_collibra_metadata()
        print(f"Asset Types: {len(metadata['AssetType'])}")
        print(f"Communities: {len(metadata['Community'])}")
        print(f"Domains: {len(metadata['Domain'])}")

        # 2. Find communities
        print("\n🏘️ Getting communities...")
        communities = connector.community.find_communities()
        if communities.get("results"):
            first_community = communities["results"][0]
            print(f"First community: {first_community['name']} ({first_community['id']})")

            # 3. Find domains in the first community
            print("\n📁 Getting domains in community...")
            domains = connector.domain.find_domains(community_id=first_community['id'])
            if domains.get("results"):
                first_domain = domains["results"][0]
                print(f"First domain: {first_domain['name']} ({first_domain['id']})")

                # 4. Find assets in the domain
                print("\n📄 Getting assets in domain...")
                assets = connector.asset.find_assets(domain_id=first_domain['id'])
                if assets.get("results"):
                    first_asset = assets["results"][0]
                    print(f"First asset: {first_asset['name']} ({first_asset['id']})")

                    # 5. Get asset details
                    print("\n🔍 Getting asset details...")
                    asset_details = connector.asset.get_asset(first_asset['id'])
                    print(f"Asset type: {asset_details.get('type', {}).get('name', 'Unknown')}")
                    # 7. Get asset activities
                    print("\n📋 Getting asset activities...")
                    activities = connector.asset.get_asset_activities(first_asset['id'], limit=5)
                    print(f"Recent activities: {len(activities)}")

        # 8. Example of creating a new asset (commented out to prevent accidental creation)
        """
        print("\n➕ Creating new asset...")
        new_asset = connector.asset.add_asset(
            name="Test Asset from Python",
            domain_id="your-domain-id-here",
            display_name="Test Asset Display Name",
            type_id="your-asset-type-id-here"
        )
        print(f"New asset created: {new_asset.get('id')}")
        """

        # 9. Get user information (if you have a user)
        print("\n👤 Getting user information...")
        try:
            user_id = connector.user.get_user_by_username("admin")  # Example username
            if user_id:
                user_info = connector.user.get_user(user_id)
                print(f"User: {user_info.get('firstName', '')} {user_info.get('lastName', '')}")
        except Exception as e:
            print(f"User lookup failed: {e}")

        # 10. Using utilities for complex operations
        print("\n🔧 Using utility functions...")
        try:
            # Get complete hierarchy for a community
            if communities.get("results"):
                hierarchy = connector.utils.get_full_asset_hierarchy(
                    community_id=first_community['id']
                )
                total_assets = sum(len(domain["assets"]) for domain in hierarchy["domains"])
                print(f"Total assets in community '{hierarchy['community']['name']}': {total_assets}")
        except Exception as e:
            print(f"Hierarchy retrieval failed: {e}")

    except Exception as e:
        print(f"❌ Error during execution: {e}")


if __name__ == "__main__":
    main()
