from database.database import init_db
from repositories.aussda_repository import process_all_aussda_projects
from repositories.ukds_repository import process_all_ukds_projects


def main():
    init_db()

    process_all_aussda_projects(query="qualitative", per_page=25)
    process_all_ukds_projects(query="qualitative", page_size=25)


if __name__ == "__main__":
    main()